import datetime
import multiprocessing
import typing
from multiprocessing.pool import ThreadPool

import pandas as pd
from influxdb import DataFrameClient
import pyiqfeed as iq
from atpy.data.cache.influxdb_cache_requests import InfluxDBCacheRequests
from atpy.data.iqfeed.iqfeed_level_1_provider import Fundamentals
from atpy.data.iqfeed.util import adjust


class IQFeedInfluxDBCacheRequests(InfluxDBCacheRequests):

    def __init__(self, client: DataFrameClient, streaming_conn: iq.QuoteConn, default_timezone: str = 'US/Eastern'):
        super().__init__(client=client, default_timezone=default_timezone)
        self.streaming_conn = streaming_conn

    def request_ohlc(self, interval_len: int, interval_type: str = 's', symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, ascending: bool = True, adjust_data=True):
        result = super().request_ohlc(interval_len=interval_len, interval_type=interval_type, symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd)
        if isinstance(result.index, pd.MultiIndex):
            pool = ThreadPool(multiprocessing.cpu_count())
            pool.map(lambda s: adjust(result, Fundamentals.get(s, self.streaming_conn)), (s for s in result.index.levels[0]))
            pool.close()
        elif isinstance(symbol, str):
            adjust(result, Fundamentals.get(symbol, self.streaming_conn))

        return result
