import datetime
import logging
import queue
import threading
import typing
from abc import abstractmethod

import numpy as np
from dateutil import tz
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from influxdb import InfluxDBClient, DataFrameClient


class BarsFilter(typing.NamedTuple):
    ticker: typing.Union[list, str]
    interval_len: int
    interval_type: str
    bgn_prd: datetime.datetime


class ClientFactory(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def new_client(self):
        return InfluxDBClient(**self.kwargs)

    def new_df_client(self):
        return DataFrameClient(**self.kwargs)


class InfluxDBCache(object):
    """
    InfluxDB bar data cache using abstract data provider
    """

    def __init__(self, client_factory: ClientFactory, listeners=None, use_stream_events=True, time_delta_back: relativedelta = relativedelta(years=5)):
        self.client_factory = client_factory
        self.listeners = listeners
        self._use_stream_events = use_stream_events
        self._time_delta_back = time_delta_back
        self._synchronized_symbols = set()
        self._lock = threading.RLock()

    def __enter__(self):
        self.client = self.client_factory.new_df_client()

        if self.listeners is not None:
            self.listeners += self.on_event

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.client.close()

        if self.listeners is not None:
            self.listeners -= self.on_event

    def on_event(self, event):
        if self._use_stream_events and event['type'] == 'bar':
            with self._lock:
                data = event['data']
                interval = str(event['interval_len']) + '_' + event['interval_type']

                if data['symbol'] not in self._synchronized_symbols:
                    self.verify_timeseries_integrity(self.client, data['symbol'], event['interval_len'], event['interval_type'])
                    self._synchronized_symbols.add(data['symbol'])

                json_body = [
                    {
                        "measurement": "bars",
                        "tags": {
                            "symbol": data['symbol'],
                            "interval": interval,
                        },

                        "time": data['timestamp'] if isinstance(data['timestamp'], datetime.datetime) else data['timestamp'].astype(datetime.datetime),
                        "fields": {k: int(v) if isinstance(v, (int, np.integer)) else v for k, v in data.items() if k not in ('timestamp', 'symbol')}
                    }
                ]

                InfluxDBClient.write_points(self.client, json_body, protocol='json', time_precision='s')

    @property
    def ranges(self):
        """
        :return: list of latest times for each entry grouped by symbol and interval
        """
        parse_time = lambda t: parse(t).replace(tzinfo=tz.gettz('UTC'))

        points = InfluxDBClient.query(self.client, "select FIRST(close), symbol, interval, time from bars group by symbol, interval").get_points()
        firsts = {(entry['symbol'], int(entry['interval'].split('_')[0]), entry['interval'].split('_')[1]): parse_time(entry['time']) for entry in points}

        points = InfluxDBClient.query(self.client, "select LAST(close), symbol, interval, time from bars group by symbol, interval").get_points()
        lasts = {(entry['symbol'], int(entry['interval'].split('_')[0]), entry['interval'].split('_')[1]): parse_time(entry['time']) for entry in points}

        result = {k: (firsts[k], lasts[k]) for k in firsts.keys() & lasts.keys()}

        return result

    def verify_timeseries_integrity(self, client: DataFrameClient, symbol: str, interval_len: int, interval_type: str = 's'):
        interval = str(interval_len) + '_' + interval_type

        cached = list(InfluxDBClient.query(client, 'select LAST(close) from bars where symbol="{}" and interval="{}"'.format(symbol, interval), chunked=True).get_points())

        if len(cached) > 0:
            d = parse(cached[0]['time']).astimezone(tz.gettz('UTC'))
        else:
            d = datetime.datetime.utcnow().replace(tzinfo=tz.gettz('UTC')) - self._time_delta_back

        to_cache = self._request_noncache_datum(symbol, d, interval_len, interval_type)

        if to_cache is not None and not to_cache.empty:
            to_cache.drop('timestamp', axis=1, inplace=True)
            to_cache['interval'] = interval

            client.write_points(to_cache, 'bars', protocol='line', tag_columns=['symbol', 'interval'], time_precision='s')

    @abstractmethod
    def _request_noncache_data(self, filters: typing.List[BarsFilter], q: queue.Queue):
        """
        :return: request data from data provider (has to be UTC localized)
        """
        pass

    @abstractmethod
    def _request_noncache_datum(self, ticker: typing.Union[list, str], bgn_prd: datetime.datetime, interval_len: int, interval_type: str = 's'):
        """
        :return: request data from data provider (has to be UTC localized)
        """
        pass

    def update_to_latest(self, new_symbols: set=None, skip_if_older_than: datetime.timedelta=None):
        """
        Update existing entries in the database to the most current values
        :param new_symbols: additional symbols to add {(symbol, interval_len, interval_type), ...}}
        :param skip_if_older_than: skip symbol update if the symbol is older than...
        :return:
        """
        filters = list()

        new_symbols = set() if new_symbols is None else new_symbols

        if skip_if_older_than is not None:
            skip_if_older_than = (datetime.datetime.utcnow().replace(tzinfo=tz.gettz('UTC')) - skip_if_older_than).astimezone(tz.gettz('US/Eastern'))

        ranges = self.ranges
        for key, time in [(e[0], e[1][1]) for e in ranges.items()]:
            if key in new_symbols:
                new_symbols.remove(key)

            if skip_if_older_than is None or time > skip_if_older_than:
                bgn_prd = datetime.datetime.combine(time.date(), datetime.datetime.min.time()).replace(tzinfo=tz.gettz('US/Eastern'))
                filters.append(BarsFilter(ticker=key[0], bgn_prd=bgn_prd, interval_len=key[1], interval_type=key[2]))

        bgn_prd = datetime.datetime.combine(datetime.datetime.utcnow().date() - self._time_delta_back, datetime.datetime.min.time()).replace(tzinfo=tz.gettz('US/Eastern'))
        for (symbol, interval_len, interval_type) in new_symbols:
            filters.append(BarsFilter(ticker=symbol, bgn_prd=bgn_prd, interval_len=interval_len, interval_type=interval_type))

        logging.getLogger(__name__).info("Updating " + str(len(filters)) + " total symbols and intervals; New symbols and intervals: " + str(len(new_symbols)))

        q = queue.Queue(maxsize=100)

        def worker():
            client = self.client_factory.new_df_client()

            try:
                for i, tupl in enumerate(iter(q.get, None)):
                    if tupl is None:
                        return

                    ft, to_cache = tupl

                    if to_cache is not None and not to_cache.empty:
                        to_cache.drop('timestamp', axis=1, inplace=True)
                        to_cache['interval'] = str(ft.interval_len) + '_' + ft.interval_type

                    try:
                        client.write_points(to_cache, 'bars', protocol='line', tag_columns=['symbol', 'interval'], time_precision='s')
                    except Exception as err:
                        logging.getLogger(__name__).exception(err)

                    if i > 0 and (i % 20 == 0 or i == len(filters)):
                        logging.getLogger(__name__).info("Cached " + str(i) + " queries")
            finally:
                client.close()

        t = threading.Thread(target=worker)
        t.start()

        self._request_noncache_data(filters, q)

        t.join()

    def add_adjustments(self, adjustments: list, data_provider: str):
        """
        add a list of splits/dividends to the database
        :param adjustments: list of adjustments of the type [(timestamp: datetime.date, symbol: str, typ: str, value), ...]
        :param data_provider: data provider
        """
        points = [self._get_adjustment_json_query(*a, data_provider) for a in adjustments]
        return InfluxDBClient.write_points(self.client, points, protocol='json', time_precision='s')

    def add_adjustment(self, timestamp: datetime.date, symbol: str, typ: str, value: float, data_provider: str):
        """
        add splits/dividends to the database
        :param timestamp: date of the adjustment
        :param symbol: symbol
        :param typ: 'split' or 'dividend'
        :param value: split_factor/dividend_rate
        :param data_provider: data provider
        """
        json_body = self._get_adjustment_json_query(timestamp=timestamp, symbol=symbol, typ=typ, value=value, data_provider=data_provider)
        return InfluxDBClient.write_points(self.client, [json_body], protocol='json', time_precision='s')

    @staticmethod
    def _get_adjustment_json_query(timestamp: datetime.date, symbol: str, typ: str, value: float, data_provider: str):
        return {
            "measurement": "splits_dividends",
            "tags": {
                "symbol": symbol,
                "data_provider": data_provider,
            },

            "time": datetime.datetime.combine(timestamp, datetime.datetime.min.time()),
            "fields": {'value': value, 'type': typ}
        }
