import datetime
import logging
import queue
import threading
import typing
from abc import abstractmethod

import numpy as np
import pytz
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from influxdb import InfluxDBClient, DataFrameClient

import pyevents.events as events


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


class InfluxDBCache(object, metaclass=events.GlobalRegister):
    """
    InfluxDB bar data cache using abstract data provider
    """

    def __init__(self, client_factory: ClientFactory, use_stream_events=True, time_delta_back: relativedelta = relativedelta(years=5)):
        self.client_factory = client_factory
        self._use_stream_events = use_stream_events
        self._time_delta_back = time_delta_back
        self._synchronized_symbols = set()
        self._lock = threading.RLock()

    def __enter__(self):
        self.client = self.client_factory.new_df_client()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.client.close()

    @events.listener
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

                InfluxDBClient.write_points(self.client, json_body, protocol='json')

    @property
    def ranges(self):
        """
        :return: list of latest times for each entry grouped by symbol and interval
        """
        parse_time = lambda t: parse(t).replace(tzinfo=pytz.utc)

        points = InfluxDBClient.query(self.client, "select FIRST(close), symbol, interval, time from bars group by symbol, interval").get_points()
        firsts = {entry['symbol'] + '_' + entry['interval']: parse_time(entry['time']) for entry in points}

        points = InfluxDBClient.query(self.client, "select LAST(close), symbol, interval, time from bars group by symbol, interval").get_points()
        lasts = {entry['symbol'] + '_' + entry['interval']: parse_time(entry['time']) for entry in points}

        result = {k: (firsts[k], lasts[k]) for k in firsts.keys() & lasts.keys()}

        return result

    def verify_timeseries_integrity(self, client: DataFrameClient, symbol: str, interval_len: int, interval_type: str = 's'):
        interval = str(interval_len) + '_' + interval_type

        cached = list(InfluxDBClient.query(client, 'select LAST(close) from bars where symbol="{}" and interval="{}"'.format(symbol, interval)).get_points())

        if len(cached) > 0:
            d = parse(cached[0]['time'])
        else:
            d = datetime.datetime.now() - self._time_delta_back

        d = d.tz_localize(pytz.utc)

        to_cache = self._request_noncache_datum(symbol, d, interval_len, interval_type)

        if to_cache is not None and not to_cache.empty:
            to_cache.drop('timestamp', axis=1, inplace=True)
            to_cache['interval'] = interval

            client.write_points(to_cache, 'bars', protocol='line', tag_columns=['symbol', 'interval'])

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

    def update_to_latest(self, new_symbols: dict=None, skip_if_older_than: datetime.timedelta=None):
        """
        Update existing entries in the database to the most current values
        :param new_symbols: additional symbols to add {symbol: {(interval_len, interval_type), ...}}
        :param skip_if_older_than: skip symbol update if the symbol is older than...
        :return:
        """
        filters = list()

        new_symbols = dict() if new_symbols is None else new_symbols

        ranges = self.ranges
        for time, symbol, interval_len, interval_type in [(e[1][1], *e[0].split('_')) for e in ranges.items()]:
            interval = (int(interval_len), interval_type)
            if symbol in new_symbols and interval in new_symbols[symbol]:
                new_symbols[symbol].remove(interval)

            filters.append(BarsFilter(ticker=symbol, bgn_prd=datetime.datetime.combine(time.date(), datetime.datetime.min.time()), interval_len=int(interval_len), interval_type=interval_type))

        d = datetime.datetime.now() - self._time_delta_back
        for symbol, interval in new_symbols.items():
            filters += [BarsFilter(ticker=symbol, bgn_prd=d, interval_len=int(i[0]), interval_type=i[1]) for i in interval]

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
                        client.write_points(to_cache, 'bars', protocol='line', tag_columns=['symbol', 'interval'])
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
