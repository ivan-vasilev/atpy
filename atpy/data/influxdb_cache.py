import pyevents.events as events
import threading
import datetime
from dateutil.parser import parse
from influxdb import InfluxDBClient, DataFrameClient
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsInPeriodFilter
from dateutil.relativedelta import relativedelta
import numpy as np


class InfluxDBCache(object, metaclass=events.GlobalRegister):
    def __init__(self, client: DataFrameClient, use_stream_events=True, history_provider: IQFeedHistoryProvider=None, time_delta_back: relativedelta=relativedelta(years=5)):
        self.client = client
        self._use_stream_events = use_stream_events
        self._history_provider = history_provider
        self._time_delta_back = time_delta_back
        self._synchronized_symbols = set()
        self._lock = threading.RLock()

    def __enter__(self):
        self.own_history = self._history_provider is None
        if self.own_history:
            self._history_provider = IQFeedHistoryProvider(num_connections=1, exclude_nan_ratio=None)
            self._history_provider.__enter__()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.own_history:
            self._history_provider.__exit__(exception_type, exception_value, traceback)

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
                            "interval_len": event['interval_len'],
                        },

                        "time": data['time_stamp'].astype(datetime.datetime),
                        "fields": {**{'interval': interval}, **{k: int(v) if isinstance(v, (int, np.integer)) else v for k, v in data.items() if k not in ('time_stamp', 'symbol')}}
                    }
                ]

                InfluxDBClient.write_points(self.client, json_body, protocol='json')

    def verify_timeseries_integrity(self, symbol: str, interval_len: int, interval_type: str):
        interval = str(interval_len) + '-' + interval_type

        cached = list(InfluxDBClient.query(self.client, 'select LAST(close) from bars where symbol="{}" and interval="{}"'.format(symbol, interval)).get_points())

        if len(cached) > 0:
            d = parse(cached[0]['time'])
        else:
            d = datetime.datetime.now() - self._time_delta_back

        f = BarsInPeriodFilter(ticker=symbol, bgn_prd=d, end_prd=None, interval_len=interval_len, interval_type=interval_type)
        to_cache = self._history_provider.request_data(f, synchronize_timestamps=False, adjust_data=False)

        if to_cache is not None and not to_cache.empty:
            to_cache.drop('time_stamp', axis=1, inplace=True)
            to_cache['interval'] = interval

            self.client.write_points(to_cache, 'bars', protocol='line', tag_columns=['symbol', 'interval'])
