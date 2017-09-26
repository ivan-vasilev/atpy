import datetime
import logging
import queue
import threading

import numpy as np
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from influxdb import InfluxDBClient, DataFrameClient

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

                        "time": data['time_stamp'].astype(datetime.datetime),
                        "fields": {k: int(v) if isinstance(v, (int, np.integer)) else v for k, v in data.items() if k not in ('time_stamp', 'symbol')}
                    }
                ]

                InfluxDBClient.write_points(self.client, json_body, protocol='json')

    @property
    def latest_entries(self):
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
            to_cache.drop('time_stamp', axis=1, inplace=True)
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
                    to_cache.drop('time_stamp', axis=1, inplace=True)
                    to_cache['interval'] = str(ft.interval_len) + '-' + ft.interval_type

                try:
                    self.client.write_points(to_cache, 'bars', protocol='line', tag_columns=['symbol', 'interval'])
                except Exception as err:
                    logging.getLogger(__name__).exception(err)

                with lock:
                    global_counter['counter'] += 1
                    gc = global_counter['counter']

                if gc % 200 == 0 or gc == len(filters):
                    logging.getLogger(__name__).info("Cached " + str(gc) + " queries")

        threads = [threading.Thread(target=worker) for _ in range(self.history_provider.num_connections)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()
