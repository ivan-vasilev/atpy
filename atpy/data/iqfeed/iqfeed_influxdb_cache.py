import logging
import os
import tempfile
import zipfile

import requests
from dateutil.relativedelta import relativedelta
from influxdb import InfluxDBClient

from atpy.data.cache.influxdb_cache import InfluxDBCache, ClientFactory
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsInPeriodFilter
from dateutil import tz


class IQFeedInfluxDBCache(InfluxDBCache):
    """
    InfluxDB bar data cache using IQFeed data provider
    """

    def __init__(self, client_factory: ClientFactory, history: IQFeedHistoryProvider = None, use_stream_events=True, time_delta_back: relativedelta = relativedelta(years=5)):
        super().__init__(client_factory=client_factory, use_stream_events=use_stream_events, time_delta_back=time_delta_back)
        self._history = history

    def __enter__(self):
        super().__enter__()

        self.own_history = self.history is None
        if self.own_history:
            self.history = IQFeedHistoryProvider(exclude_nan_ratio=None)
            self.history.__enter__()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        super().__exit__(exception_type, exception_value, traceback)

        if self.own_history:
            self.history.__exit__(exception_type, exception_value, traceback)

    @property
    def history(self):
        return self._history

    @history.setter
    def history(self, x):
        self._history = x

    def _request_noncache_datum(self, symbol, bgn_prd, interval_len, interval_type='s'):
        if bgn_prd is not None:
            bgn_prd = bgn_prd.astimezone(tz.gettz('US/Eastern'))

        f = BarsInPeriodFilter(ticker=symbol, bgn_prd=bgn_prd, end_prd=None, interval_len=interval_len, interval_type=interval_type)
        return self.history.request_data(f, synchronize_timestamps=False, adjust_data=False)

    def _request_noncache_data(self, filters, q):
        new_filters = list()
        for f in filters:
            if f.bgn_prd is not None:
                new_filters.append(BarsInPeriodFilter(ticker=f.ticker, bgn_prd=f.bgn_prd.astimezone(tz.gettz('US/Eastern')), end_prd=None, interval_len=f.interval_len, interval_type=f.interval_type))
            else:
                new_filters.append(BarsInPeriodFilter(ticker=f.ticker, bgn_prd=f.bgn_prd, end_prd=None, interval_len=f.interval_len, interval_type=f.interval_type))

        self.history.request_data_by_filters(new_filters, q, adjust_data=False)

    def get_missing_symbols(self, intervals, symbols_file: str = None):
        """
        :param intervals: [(interval_len, interval_type), ...]
        :param symbols_file: Symbols zip file location to prevent download every time
        """

        with tempfile.TemporaryDirectory() as td:
            if symbols_file is not None:
                logging.getLogger(__name__).info("Symbols: " + symbols_file)
                zipfile.ZipFile(symbols_file).extractall(td)
            else:
                with tempfile.TemporaryFile() as tf:
                    logging.getLogger(__name__).info("Downloading symbol list... ")
                    tf.write(requests.get('http://www.dtniq.com/product/mktsymbols_v2.zip', allow_redirects=True).content)
                    zipfile.ZipFile(tf).extractall(td)

            with open(os.path.join(td, 'mktsymbols_v2.txt')) as f:
                content = f.readlines()

        content = [c for c in content if '\tEQUITY' in c and ('\tNYSE' in c or '\tNASDAQ' in c)]

        all_symbols = {s.split('\t')[0] for s in content}

        result = dict()
        for i in intervals:
            existing_symbols = {e['symbol'] for e in InfluxDBClient.query(self.client, "select FIRST(close), symbol from bars where interval = '{}' group by symbol".format(str(i[0]) + '_' + i[1])).get_points()}

            for s in all_symbols - existing_symbols:
                if s not in result:
                    result[s] = set()

                result[s].add(i)

        return result
