import datetime
import logging
import multiprocessing
import os
import queue
import tempfile
import threading
import typing
import zipfile
from abc import abstractmethod

import numpy as np
import requests
from dateutil import tz
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from influxdb import InfluxDBClient, DataFrameClient

import pyevents.events as events


class BarsFilter(typing.NamedTuple):
    ticker: typing.Union[list, str]
    interval_len: int
    interval_type: str
    bgn_prd: datetime.datetime


class InfluxDBCache(object, metaclass=events.GlobalRegister):
    """
    InfluxDB bar data cache using abstract data provider
    """

    def __init__(self, client: DataFrameClient, use_stream_events=True, time_delta_back: relativedelta = relativedelta(years=5), default_timezone: str = 'US/Eastern'):
        self.client = client
        self._use_stream_events = use_stream_events
        self._time_delta_back = time_delta_back
        self._default_timezone = default_timezone
        self._synchronized_symbols = set()
        self._lock = threading.RLock()

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
        parse_time = lambda t: parse(t) if self._default_timezone is None else parse(t).replace(tzinfo=tz.gettz(self._default_timezone))

        points = InfluxDBClient.query(self.client, "select FIRST(close), time from bars group by symbol, interval").get_points()
        firsts = {entry['first']['Tags']['symbol'] + '_' + entry['first']['Tags']['interval']: parse_time(entry['time']) for entry in points}

        points = InfluxDBClient.query(self.client, "select LAST(close), time from bars group by symbol, interval").get_points()
        lasts = {entry['last']['Tags']['symbol'] + '_' + entry['last']['Tags']['interval']: parse_time(entry['time']) for entry in points}

        result = {k: (firsts[k], lasts[k]) for k in firsts.keys() & lasts.keys()}

        return result

    def verify_timeseries_integrity(self, symbol: str, interval_len: int, interval_type: str = 's'):
        interval = str(interval_len) + '_' + interval_type

        cached = list(InfluxDBClient.query(self.client, 'select LAST(close) from bars where symbol="{}" and interval="{}"'.format(symbol, interval)).get_points())

        if len(cached) > 0:
            d = parse(cached[0]['time'])
        else:
            d = datetime.datetime.now() - self._time_delta_back

        if self._default_timezone is not None:
            d = d.replace(tzinfo=tz.gettz(self._default_timezone))

        to_cache = self._request_noncache_datum(symbol, d, interval_len, interval_type)

        if to_cache is not None and not to_cache.empty:
            to_cache.drop('timestamp', axis=1, inplace=True)
            to_cache['interval'] = interval

            self.client.write_points(to_cache, 'bars', protocol='line', tag_columns=['symbol', 'interval'])

    @abstractmethod
    def _request_noncache_data(self, filters: typing.List[BarsFilter], q: queue.Queue):
        pass

    @abstractmethod
    def _request_noncache_datum(self, ticker: typing.Union[list, str], bgn_prd: datetime.datetime, interval_len: int, interval_type: str = 's'):
        pass

    def update_to_latest(self, new_symbols: dict = None):
        """
        Update existing entries in the database to the most current values
        :param new_symbols: additional symbols to add {symbol: [(interval_len, interval_type), ...]}
        :return:
        """
        filters = list()

        new_symbols = set() if new_symbols is None else new_symbols

        for time, symbol, interval_len, interval_type in [(e[1][1], *e[0].split('_')) for e in self.ranges.items()]:
            if symbol in new_symbols and str(new_symbols[symbol][0]) == interval_len and new_symbols[symbol][1] == interval_type:
                del new_symbols[symbol]

            filters.append(BarsFilter(ticker=symbol, bgn_prd=time, interval_len=int(interval_len), interval_type=interval_type))

        d = datetime.datetime.now() - self._time_delta_back
        for symbol, interval in new_symbols.items():
            filters += [BarsFilter(ticker=symbol, bgn_prd=d, interval_len=int(i[0]), interval_type=i[1]) for i in interval]

        q = queue.Queue()
        self._request_noncache_data(filters, q)

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

        threads = [threading.Thread(target=worker) for _ in range(multiprocessing.cpu_count())]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

    def get_missing_symbols(self, intervals):
        """
        :param intervals: [(interval_len, interval_type), ...]
        """
        with tempfile.TemporaryFile() as tf, tempfile.TemporaryDirectory() as td:
            r = requests.get('http://www.dtniq.com/product/mktsymbols_v2.zip', allow_redirects=True)
            tf.write(r.content)

            zip_ref = zipfile.ZipFile(tf)
            zip_ref.extractall(td)

            with open(os.path.join(td, 'mktsymbols_v2.txt')) as f:
                content = f.readlines()

        content = [c for c in content if '\tEQUITY' in c and ('\tNYSE' in c or '\tNASDAQ' in c)]

        all_symbols = {s.split('\t')[0] for s in content}

        result = dict()
        for i in intervals:
            existing_symbols = {e['symbol'] for e in InfluxDBClient.query(self.client, "select FIRST(close), symbol from bars where interval = '{}' group by symbol".format(str(i[0]) + '_' + i[1])).get_points()}

            for s in all_symbols - existing_symbols:
                if s not in result:
                    result[s] = list()

                result[s].append(i)

        return result

    def populate_db(self, intervals):
        """
        :param intervals: [(interval_len, interval_type), ...]
        """
        missing = self.get_missing_symbols(intervals)
        self.update_to_latest(missing)
