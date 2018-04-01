import datetime
import typing

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from influxdb import DataFrameClient

import atpy.data.iqfeed.bar_util as bars
from atpy.data.ts_util import slice_periods


class InfluxDBOHLCRequest(object):

    def __init__(self, client: DataFrameClient, interval_len: int, interval_type: str = 's', listeners=None):
        """
        :param client: influxdb client
        :param interval_len: interval length
        :param interval_type: interval type
        """
        self.interval_len = interval_len
        self.interval_type = interval_type
        self.client = client
        self.listeners = listeners

        if self.listeners is not None:
            self.listeners += self.on_event

    def on_event(self, event):
        if event['type'] == 'request_ohlc' and self.listeners is not None:
            data = self.request(**event['data'])
            self.listeners({'type': 'cache_result', 'data': data})

    def _request_raw_data(self, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, ascending: bool = True):
        """
        :param symbol: symbol or symbol list
        :param bgn_prd: start datetime (excluding)
        :param end_prd: end datetime (excluding)
        :param ascending: asc/desc
        :return: data from the database
        """

        query = "SELECT * FROM bars" + \
                _query_where(interval_len=self.interval_len, interval_type=self.interval_type, symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd) + \
                " ORDER BY time " + "ASC" if ascending else "DESC"

        result = self.client.query(query, chunked=True)
        if len(result) == 0:
            result = None
        else:
            result = result['bars']
            result.drop('interval', axis=1, inplace=True)
            result.index.name = 'timestamp'

            for c in [c for c in result.columns if result[c].dtype == np.int64]:
                result[c] = result[c].astype(np.uint64, copy=False)

            result['timestamp'] = result.index

            if len(result['symbol'].unique()) > 1:
                result.set_index('symbol', drop=False, append=True, inplace=True)
                result.sort_index(inplace=True, ascending=ascending)

            result = result[[c for c in ['open', 'high', 'low', 'close', 'period_volume', 'timestamp', 'symbol'] if c in result.columns]]

        return result

    def _postprocess_data(self, data):
        return data

    def request(self, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, ascending: bool = True, synchronize_timestamps: bool = False):
        data = self._request_raw_data(symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd, ascending=ascending)

        if synchronize_timestamps:
            data = bars.synchronize_timestamps(data)

        return data, self._postprocess_data(data)


class InfluxDBValueRequest(object):
    """abstract class for single value selection"""

    def __init__(self, value: str, client: DataFrameClient, interval_len: int, interval_type: str = 's', listeners=None):
        """
        :param value: value to select. value is a part of query
        :param client: influxdb client
        :param interval_len: interval length
        :param interval_type: interval type
        :param listeners: listeners
        """
        self.value = value
        self.interval_len = interval_len
        self.interval_type = interval_type
        self.client = client
        self.listeners = listeners

        if self.listeners is not None:
            self.listeners += self.on_event

        self.means = None
        self.stddev = None

    def on_event(self, event):
        if event['type'] == 'request_value':
            data = self.request(**event['data'])
            self.listeners({'type': 'cache_result', 'data': data})

    def _request_raw_data(self, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, ascending: bool = True):
        """
        :param symbol: symbol or symbol list
        :param bgn_prd: start datetime (excluding)
        :param end_prd: end datetime (excluding)
        :param ascending: asc/desc
        :return: data from the database
        """

        query = "SELECT symbol, " + self.value + " FROM bars" + \
                _query_where(interval_len=self.interval_len, interval_type=self.interval_type, symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd) + \
                " ORDER BY time " + "ASC" if ascending else "DESC"

        result = self.client.query(query, chunked=True)
        if len(result) == 0:
            result = None
        else:
            result = result['bars']
            result.index.name = 'timestamp'

            for c in [c for c in result.columns if result[c].dtype == np.int64]:
                result[c] = result[c].astype(np.uint64, copy=False)

            result['timestamp'] = result.index

            if len(result['symbol'].unique()) > 1:
                result.set_index('symbol', drop=False, append=True, inplace=True)
                result = result.swaplevel(0, 1, axis=0)
                result.sort_index(inplace=True, ascending=ascending)

        return result

    def _postprocess_data(self, data):
        if self.means is not None or self.stddev is not None:
            data = data.copy(deep=True)

        if len(data['symbol'].unique()) > 1:
            if self.means is not None:
                data['delta'] = data['delta'].groupby(level=0).apply(lambda x: x - self.means[x.name])

            if self.stddev is not None:
                data['delta'] = data['delta'].groupby(level=0).apply(lambda x: x / self.stddev[x.name])
        else:
            if self.means is not None:
                data['delta'] = data['delta'] - self.means[data['symbol'][0]]

            if self.stddev is not None:
                data['delta'] = data['delta'] / self.stddev[data['symbol'][0]]

        return data

    def request(self, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, ascending: bool = True, synchronize_timestamps: bool = False):
        data = self._request_raw_data(symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd, ascending=ascending)

        if synchronize_timestamps:
            data = bars.synchronize_timestamps(data)

        return data, self._postprocess_data(data)

    def enable_mean(self, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None):
        """
        :param symbol: symbol or symbol list
        :param bgn_prd: start datetime (excluding)
        :param end_prd: end datetime (excluding)
        :return: data from the database
        """
        query = "SELECT MEAN(delta) FROM (SELECT symbol, (close - open) / open as delta FROM bars" + \
                _query_where(interval_len=self.interval_len, interval_type=self.interval_type, symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd) + \
                ") GROUP BY symbol"

        rs = super(DataFrameClient, self.client).query(query, chunked=True)
        self.means = {k[1]['symbol']: next(data)['mean'] for k, data in rs.items()}

    def enable_stddev(self, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None):
        """
        :param symbol: symbol or symbol list
        :param bgn_prd: start datetime (excluding)
        :param end_prd: end datetime (excluding)
        :return: data from the database
        """
        query = "SELECT STDDEV(delta) FROM (SELECT symbol, (close - open) / open as delta FROM bars" + \
                _query_where(interval_len=self.interval_len, interval_type=self.interval_type, symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd) + \
                ") GROUP BY symbol"

        rs = super(DataFrameClient, self.client).query(query, chunked=True)
        self.stddev = {k[1]['symbol']: next(data)['stddev'] for k, data in rs.items()}


def get_adjustments(client: DataFrameClient, symbol: typing.Union[list, str] = None, typ: str = None, provider: str = None):
    query = "SELECT * FROM splits_dividends"

    where = list()
    if symbol is not None:
        if isinstance(symbol, list) and len(symbol) > 0:
            where.append("symbol =~ /{}/".format("|".join(['^' + s + '$' for s in symbol])))
        elif isinstance(symbol, str) and len(symbol) > 0:
            where.append("symbol = '{}'".format(symbol))

    if typ is not None:
        where.append("type='{}'".format(typ))

    if provider is not None:
        where.append("provider='{}'".format(provider))

    if len(where) > 0:
        query += " WHERE " + " AND ".join(where)

    result = DataFrameClient.query(client, query)
    if result:
        result = result['splits_dividends']
        result.set_index(['symbol', 'type', 'provider'], inplace=True, drop=True, append=True)
        result.sort_index(inplace=True)

        return result

    return pd.DataFrame()


def _query_where(interval_len: int, interval_type: str, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None):
    """
    generate query where string
    :param interval_len: interval length
    :param interval_type: interval type
    :param symbol: symbol or symbol list
    :param bgn_prd: start datetime (including)
    :param end_prd: end datetime (excluding)
    :return: data from the database
    """

    result = " WHERE" \
             " interval = '{}'" + \
             ('' if symbol is None else " AND symbol =" + ("~ /{}/ " if isinstance(symbol, list) else " '{}'")) + \
             ('' if bgn_prd is None else " AND time >= '{}'") + \
             ('' if end_prd is None else " AND time < '{}'")

    bgn_prd = bgn_prd.replace(tzinfo=None) if bgn_prd is not None else None
    end_prd = end_prd.replace(tzinfo=None) if end_prd is not None else None
    args = tuple(filter(lambda x: x is not None, [str(interval_len) + '_' + interval_type, None if symbol is None else "|".join(['^' + s + '$' for s in symbol]) if isinstance(symbol, list) else symbol, bgn_prd, end_prd]))
    return result.format(*args)


class BarsInPeriodProvider(object):
    """
    OHLCV Bars in period provider
    """

    def __init__(self, influxdb_cache: InfluxDBOHLCRequest, bgn_prd: datetime.datetime, delta: relativedelta, symbol: typing.Union[list, str] = None, ascend: bool = True, overlap: relativedelta = None):
        self._periods = slice_periods(bgn_prd=bgn_prd, delta=delta, ascend=ascend, overlap=overlap)

        self.influxdb_cache = influxdb_cache
        self.symbol = symbol
        self.ascending = ascend

    def __iter__(self):
        self._deltas = -1
        return self

    def __next__(self):
        self._deltas += 1

        if self._deltas < len(self._periods):
            return self.influxdb_cache.request(symbol=self.symbol, bgn_prd=self._periods[self._deltas][0], end_prd=self._periods[self._deltas][1], ascending=self.ascending)
        else:
            raise StopIteration
