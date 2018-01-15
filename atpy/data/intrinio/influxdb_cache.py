import datetime
import logging
import threading
import typing

import numpy as np
from dateutil import tz
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from influxdb import InfluxDBClient, DataFrameClient

from atpy.data.intrinio.api import get_data, historical_data_processor


class ClientFactory(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def new_client(self):
        return InfluxDBClient(**self.kwargs)

    def new_df_client(self):
        return DataFrameClient(**self.kwargs)


class InfluxDBCache(object):
    """
    InfluxDB Intrinio cache using abstract data provider
    """

    def __init__(self, client_factory: ClientFactory, listeners=None, time_delta_back: relativedelta = relativedelta(years=5)):
        self.client_factory = client_factory
        self.listeners = listeners
        self._time_delta_back = time_delta_back
        self._synchronized_symbols = set()
        self._lock = threading.RLock()

    def __enter__(self):
        self.client = self.client_factory.new_df_client()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.client.close()

    @property
    def ranges(self):
        """
        :return: list of latest times for each entry grouped by symbol and tag
        """
        parse_time = lambda t: parse(t).replace(tzinfo=tz.gettz('UTC'))

        points = InfluxDBClient.query(self.client, "select FIRST(close), symbol, itag, time from intrinio_tags group by symbol, itag").get_points()
        firsts = {(entry['symbol'], entry['itag']): parse_time(entry['time']) for entry in points}

        points = InfluxDBClient.query(self.client, "select LAST(close), symbol, itag, time from intrinio_tags group by symbol, itag").get_points()
        lasts = {(entry['symbol'], entry['itag']): parse_time(entry['time']) for entry in points}

        result = {k: (firsts[k], lasts[k]) for k in firsts.keys() & lasts.keys()}

        return result

    def _request_noncache_data(self, filters: typing.List[dict], async=False):
        """
        request filter data
        :param filters: list of dicts for data request
        :return:
        """
        return get_data(filters=filters, async=async, processor=historical_data_processor)

    def update_to_latest(self, new_symbols: typing.Set[typing.Tuple] = None, skip_if_older_than: datetime.timedelta = None):
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
                filters.append({'identifier': key[0], 'item': key[1], 'start_date': time.date(), 'endpoint': 'historical_data.csv'})

        start_date = datetime.datetime.utcnow().date() - self._time_delta_back
        for (symbol, tag) in new_symbols:
            filters.append({'identifier': symbol, 'item': tag, 'start_date': start_date.strftime('%Y-%m-%d'), 'endpoint': 'historical_data.csv'})

        logging.getLogger(__name__).info("Updating " + str(len(filters)) + " total symbols and intervals; New symbols and intervals: " + str(len(new_symbols)))

        q = self._request_noncache_data(filters, async=True)

        def worker():
            client = self.client_factory.new_df_client()

            try:
                for i, tupl in enumerate(iter(q.get, None)):
                    if tupl is None:
                        return

                    s, to_cache = tupl

                    if to_cache is not None and not to_cache.empty:
                        to_cache['symbol'] = s
                        to_cache.reset_index(level=['tag'], inplace=True)
                        to_cache.rename(columns={'tag': 'itag'}, inplace=True)
                    try:
                        client.write_points(to_cache, 'intrinio_tags', protocol='line', tag_columns=['symbol', 'itag'], time_precision='s')
                    except Exception as err:
                        logging.getLogger(__name__).exception(err)

                    if i > 0 and (i % 20 == 0 or i == len(filters)):
                        logging.getLogger(__name__).info("Cached " + str(i) + " queries")
            finally:
                client.close()

        t = threading.Thread(target=worker)
        t.start()
        t.join()

    def request_data(self, symbols: typing.Union[set, str] = None, tags: typing.Union[set, str] = None, start_date: datetime.date = None, end_date: datetime.date = None):
        query = "SELECT * FROM intrinio_tags"

        where = list()
        if symbols is not None:
            if isinstance(symbols, set) and len(symbols) > 0:
                where.append("symbol =~ /{}/".format("|".join(['^' + s + '$' for s in symbols])))
            elif isinstance(symbols, str) and len(symbols) > 0:
                where.append("symbol = '{}'".format(symbols))

        if tags is not None:
            if isinstance(tags, set) and len(tags) > 0:
                where.append("itag =~ /{}/".format("|".join(['^' + s + '$' for s in tags])))
            elif isinstance(symbols, str) and len(tags) > 0:
                where.append("itag = '{}'".format(tags))

        if start_date is not None:
            start_date = datetime.datetime.combine(start_date, datetime.datetime.min.time())
            where.append("time >= '{}'".format(start_date))

        if end_date is not None:
            end_date = datetime.datetime.combine(end_date, datetime.datetime.min.time())
            where.append("time <= '{}'".format(end_date))

        if len(where) > 0:
            query += " WHERE " + " AND ".join(where)

        result = self.client.query(query, chunked=True)

        if len(result) > 0:
            result = result['intrinio_tags']

            result.rename(columns={'itag': 'tag'}, inplace=True)
            result.set_index(['tag', 'symbol'], drop=True, inplace=True, append=True)
            result.index.rename('date', level=0, inplace=True)
            result = result.reorder_levels(['symbol', 'date', 'tag'])
            result.sort_index(inplace=True)

            if result['value'].dtype != np.float:
                result['value'] = result['value'].astype(np.float)
        else:
            result = None

        return result
