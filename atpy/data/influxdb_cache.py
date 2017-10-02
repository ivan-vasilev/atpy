import datetime
import logging
import queue
import threading

import numpy as np
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from influxdb import InfluxDBClient, DataFrameClient

import typing
import pyevents.events as events
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsInPeriodFilter


class InfluxDBCache(object, metaclass=events.GlobalRegister):
    def __init__(self, client: DataFrameClient, use_stream_events=True, history_provider: IQFeedHistoryProvider=None, time_delta_back: relativedelta=relativedelta(years=5)):
        self.client = client
        self.history_provider = history_provider
        self._use_stream_events = use_stream_events
        self._time_delta_back = time_delta_back
        self._synchronized_symbols = set()
        self._lock = threading.RLock()

    def __enter__(self):
        self.own_history = self.history_provider is None
        if self.own_history:
            self.history_provider = IQFeedHistoryProvider(exclude_nan_ratio=None)
            self.history_provider.__enter__()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.own_history:
            self.history_provider.__exit__(exception_type, exception_value, traceback)

    @events.listener
    def on_event(self, event):
        if self._use_stream_events and event['type'] == 'bar':
            with self._lock:
                data = event['data']
                interval = str(event['interval_len']) + '-' + event['interval_type']

                if data['symbol'] not in self._synchronized_symbols:
                    self.verify_timeseries_integrity(data['symbol'], event['interval_len'], event['interval_type'])
                    self._synchronized_symbols.add(data['symbol'])

                json_body = [
                    {
                        "measurement": "bars",
                        "tags": {
                            "symbol": data['symbol'],
                            "interval": interval,
                        },

                        "time": data['timestamp'].astype(datetime.datetime),
                        "fields": {k: int(v) if isinstance(v, (int, np.integer)) else v for k, v in data.items() if k not in ('timestamp', 'symbol')}
                    }
                ]

                InfluxDBClient.write_points(self.client, json_body, protocol='json')

    def request_data(self, symbol: str, interval_len: int, interval_type: str, bgn_prd: datetime.datetime=None, end_prd:datetime.datetime=None, ascending: bool=True):
        """
        :param symbol: symbol
        :param interval_len: interval length
        :param interval_type: interval type
        :param bgn_prd: start datetime (excluding)
        :param end_prd: end datetime (excluding)
        :param ascending: asc/desc
        :return: data from the database
        """
        query = "SELECT * FROM bars WHERE symbol = '{}' AND interval = '{}'" + ('' if bgn_prd is None else " AND time > '{}'") + ('' if end_prd is None else " AND time < '{}'") + " ORDER BY time {}"
        args = tuple(filter(lambda x: x is not None, [symbol, str(interval_len) + '-' + interval_type, bgn_prd, end_prd, 'ASC' if ascending else 'DESC']))
        result = self.client.query(query.format(*args))
        if len(result) == 0:
            result = None
        else:
            result = result['bars']
            result.drop('interval', axis=1, inplace=True)
            result.index.name = 'timestamp'
            result = result[['open', 'high', 'low', 'close', 'total_volume', 'period_volume', 'number_of_trades', 'symbol']]
            result.index = result.index.tz_localize(None)

            for c in [c for c in result.columns if result[c].dtype == np.int64]:
                result[c] = result[c].astype(np.uint64, copy=False)

        return result

    def series_ranges(self, symbol: typing.Union[list, str], interval_len: int, interval_type: str):
        """
        :return: list of existing ranges for symbol/interval
        """
        query = "SELECT FIRST(close), time FROM bars WHERE interval = '{}' AND symbol =" + ("~ /{}/ GROUP BY symbol" if isinstance(symbol, list) else " '{}'")
        args = tuple(filter(lambda x: x is not None, [str(interval_len) + '-' + interval_type, "|" + "|".join(symbol) if isinstance(symbol, list) else symbol]))

        firsts = InfluxDBClient.query(self.client, query.format(*args))

        query = "SELECT LAST(close), time FROM bars WHERE interval = '{}' AND symbol =" + ("~ /{}/ GROUP BY symbol" if isinstance(symbol, list) else " '{}'")
        lasts = InfluxDBClient.query(self.client, query.format(*args))

        if len(firsts) == 0:
            result = None
        else:
            firsts = {entry['first']['Tags']['symbol']: parse(entry['time'][:-1]) for entry in firsts.get_points()}
            lasts = {entry['last']['Tags']['symbol']: parse(entry['time'][:-1]) for entry in lasts.get_points()}
            result = {k: (firsts[k], lasts[k]) for k in firsts.keys()}

        return result if len(result) > 1 else list(result.values())[0]

    @property
    def latest_entries(self):
        """
        :return: list of latest times for each entry grouped by symbol and interval
        """
        result = [(entry['time'], entry['last']['Tags']) for entry in InfluxDBClient.query(self.client, "select LAST(close), time from bars group by symbol, interval").get_points()]
        result.sort(key=lambda e: (e[1]['symbol'], e[1]['interval'], e[0]))
        return result

    def verify_timeseries_integrity(self, symbol: str, interval_len: int, interval_type: str):
        interval = str(interval_len) + '-' + interval_type

        cached = list(InfluxDBClient.query(self.client, 'select LAST(close) from bars where symbol="{}" and interval="{}"'.format(symbol, interval)).get_points())

        if len(cached) > 0:
            d = parse(cached[0]['time'])
        else:
            d = datetime.datetime.now() - self._time_delta_back

        f = BarsInPeriodFilter(ticker=symbol, bgn_prd=d, end_prd=None, interval_len=interval_len, interval_type=interval_type)
        to_cache = self.history_provider.request_data(f, synchronize_timestamps=False, adjust_data=False)

        if to_cache is not None and not to_cache.empty:
            to_cache.drop('timestamp', axis=1, inplace=True)
            to_cache['interval'] = interval

            self.client.write_points(to_cache, 'bars', protocol='line', tag_columns=['symbol', 'interval'])

    def update_to_latest(self):
        """
        Update existing entries in the database to the most current values
        """
        filters = list()
        for time, tags in self.latest_entries:
            interval_len, interval_type = tags['interval'].split('-')
            filters.append(BarsInPeriodFilter(ticker=tags['symbol'], bgn_prd=parse(time), end_prd=None, interval_len=int(interval_len), interval_type=interval_type))

        q = queue.Queue()
        self.history_provider.request_data_by_filters(filters, q, adjust_data=False)

        lock = threading.Lock()

        stop_request = threading.Event()

        global_counter = {'counter': 0}

        def worker():
            while not stop_request.is_set():
                tupl = q.get()

                if tupl is None:
                    stop_request.set()
                    return
                else:
                    ft, to_cache = tupl

                if to_cache is not None and not to_cache.empty:
                    to_cache.drop('timestamp', axis=1, inplace=True)
                    to_cache['interval'] = str(ft.interval_len) + '-' + ft.interval_type

                try:
                    self.client.write_points(to_cache, 'bars', protocol='line', tag_columns=['symbol', 'interval'])
                except Exception as err:
                    logging.getLogger(__name__).exception(err)

                with lock:
                    global_counter['counter'] += 1
                    gc = global_counter['counter']

                if gc % 1 == 0 or gc == len(filters):
                    logging.getLogger(__name__).info("Cached " + str(gc) + " queries")

        threads = [threading.Thread(target=worker) for _ in range(self.history_provider.num_connections)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()
