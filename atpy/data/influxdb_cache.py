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
from dateutil import tz


class InfluxDBCache(object, metaclass=events.GlobalRegister):
    def __init__(self, client: DataFrameClient, use_stream_events=True, history_provider: IQFeedHistoryProvider=None, time_delta_back: relativedelta=relativedelta(years=5), default_timezone: str='US/Eastern'):
        self.client = client
        self.history_provider = history_provider
        self._use_stream_events = use_stream_events
        self._time_delta_back = time_delta_back
        self._default_timezone = default_timezone
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
                interval = str(event['interval_len']) + '_' + event['interval_type']

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

    def request_data(self, interval_len: int, interval_type: str, symbol: typing.Union[list, str]=None, bgn_prd: datetime.datetime=None, end_prd:datetime.datetime=None, ascending: bool=True):
        """
        :param interval_len: interval length
        :param interval_type: interval type
        :param symbol: symbol or symbol list
        :param bgn_prd: start datetime (excluding)
        :param end_prd: end datetime (excluding)
        :param ascending: asc/desc
        :return: data from the database
        """

        query = "SELECT * FROM bars WHERE " \
                "interval = '{}'" + \
                ('' if symbol is None else " AND symbol =" + ("~ /{}/ " if isinstance(symbol, list) else " '{}'")) + \
                ('' if bgn_prd is None else " AND time > '{}'") + \
                ('' if end_prd is None else " AND time < '{}'") + \
                " ORDER BY time {}"

        args = tuple(filter(lambda x: x is not None, [str(interval_len) + '_' + interval_type, None if symbol is None else "|".join(symbol) if isinstance(symbol, list) else symbol, bgn_prd, end_prd, 'ASC' if ascending else 'DESC']))
        result = self.client.query(query.format(*args))
        if len(result) == 0:
            result = None
        else:
            result = result['bars']
            result.drop('interval', axis=1, inplace=True)
            result.index.name = 'timestamp'
            result = result[['open', 'high', 'low', 'close', 'total_volume', 'period_volume', 'number_of_trades', 'symbol']]

            if self._default_timezone is not None:
                result.index = result.index.tz_convert(self._default_timezone)

            for c in [c for c in result.columns if result[c].dtype == np.int64]:
                result[c] = result[c].astype(np.uint64, copy=False)

            if len(result['symbol'].unique()) > 1:
                result['timestamp'] = result.index
                result.set_index('symbol', drop=False, append=True, inplace=True)
                result = result.swaplevel(0, 1, axis=0)
                result.sort_index(inplace=True, ascending=ascending)
                result = result[['open', 'high', 'low', 'close', 'total_volume', 'period_volume', 'number_of_trades', 'timestamp', 'symbol']]

        return result

    @property
    def ranges(self):
        """
        :return: list of latest times for each entry grouped by symbol and interval
        """
        parse_time = lambda t: parse(t) if self._default_timezone is None else parse(t).replace(tzinfo=tz.gettz(self._default_timezone))

        points = InfluxDBClient.query(self.client, "select FIRST(close), time from bars group by symbol, interval").get_points()
        firsts = {entry['first']['Tags']['symbol'] + '_' + entry['first']['Tags']['interval']: parse_time(entry['time']) for entry in points}

        points = InfluxDBClient.query(self.client, "select LAST(close), time from bars group by symbol, interval").get_points()
        lasts = {entry['last']['Tags']['symbol'] + '_' + entry['last']['Tags']['interval']: parse_time(entry['time']) for entry in points}

        result = {k: (firsts[k], lasts[k]) for k in firsts.keys() & lasts.keys()}

        return result

    def verify_timeseries_integrity(self, symbol: str, interval_len: int, interval_type: str):
        interval = str(interval_len) + '_' + interval_type

        cached = list(InfluxDBClient.query(self.client, 'select LAST(close) from bars where symbol="{}" and interval="{}"'.format(symbol, interval)).get_points())

        if len(cached) > 0:
            d = parse(cached[0]['time'])
        else:
            d = datetime.datetime.now() - self._time_delta_back

        if self._default_timezone is not None:
            d = d.replace(tzinfo=tz.gettz(self._default_timezone))

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
        for time, symbol, interval_len, interval_type in [(e[1][1], *e[0].split('_')) for e in self.ranges.items()]:
            filters.append(BarsInPeriodFilter(ticker=symbol, bgn_prd=time, end_prd=None, interval_len=int(interval_len), interval_type=interval_type))

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
                    to_cache['interval'] = str(ft.interval_len) + '_' + ft.interval_type

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
