import datetime
import typing

import numpy as np
from influxdb import DataFrameClient

import atpy.data.iqfeed.bar_util as bars
import pyevents.events as events


class InfluxDBOHLCRequest(object, metaclass=events.GlobalRegister):
    def __init__(self, client: DataFrameClient, interval_len: int, interval_type: str = 's'):
        """
        :param client: influxdb client
        :param interval_len: interval length
        :param interval_type: interval type
        """
        self.interval_len = interval_len
        self.interval_type = interval_type
        self.client = client

    @events.listener
    def on_event(self, event):
        if event['type'] == 'request_ohlc':
            self.request_result(self.request(**event['data']))

    @events.after
    def request_result(self, data):
        return {'type': 'cache_result', 'data': data}

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

        result = self.client.query(query)
        if len(result) == 0:
            result = None
        else:
            result = result['bars']
            result.drop('interval', axis=1, inplace=True)
            result.index.name = 'timestamp'
            result = result[['open', 'high', 'low', 'close', 'total_volume', 'period_volume', 'number_of_trades', 'symbol']]

            for c in [c for c in result.columns if result[c].dtype == np.int64]:
                result[c] = result[c].astype(np.uint64, copy=False)

            result['timestamp'] = result.index

            if len(result['symbol'].unique()) > 1:
                result.set_index('symbol', drop=False, append=True, inplace=True)
                result = result.swaplevel(0, 1, axis=0)
                result.sort_index(inplace=True, ascending=ascending)

            result = result[['open', 'high', 'low', 'close', 'total_volume', 'period_volume', 'number_of_trades', 'timestamp', 'symbol']]

        return result

    def _postprocess_data(self, data):
        return data

    def request(self, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, ascending: bool = True, synchronize_timestamps: bool = False):
        data = self._request_raw_data(symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd, ascending=ascending)

        if synchronize_timestamps:
            data = bars.synchronize_timestamps(data)

        return data, self._postprocess_data(data)


class InfluxDBValueRequest(object, metaclass=events.GlobalRegister):
    """abstract class for single value selection"""

    def __init__(self, value: str, client: DataFrameClient, interval_len: int, interval_type: str = 's'):
        """
        :param value: value to select. value is a part of query
        :param client: influxdb client
        :param interval_len: interval length
        :param interval_type: interval type
        """
        self.value = value
        self.interval_len = interval_len
        self.interval_type = interval_type
        self.client = client
        self.means = None
        self.stddev = None

    @events.listener
    def on_event(self, event):
        if event['type'] == 'request_value':
            self.request_result(self.request(**event['data']))

    @events.after
    def request_result(self, data):
        return {'type': 'cache_result', 'data': data}

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

        result = self.client.query(query)
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
        query = "SELECT MEAN(value) FROM (SELECT symbol, " + self.value + " as value FROM bars" + \
                _query_where(interval_len=self.interval_len, interval_type=self.interval_type, symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd) + \
                ") GROUP BY symbol"

        rs = super(DataFrameClient, self.client).query(query)
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

        rs = super(DataFrameClient, self.client).query(query)
        self.stddev = {k[1]['symbol']: next(data)['stddev'] for k, data in rs.items()}


class InfluxDBDeltaAdjustedRequest(InfluxDBValueRequest, metaclass=events.GlobalRegister):
    def __init__(self, client: DataFrameClient, interval_len: int, interval_type: str = 's'):
        super().__init__(value='(close - open) / open as delta, period_volume, total_volume', client=client, interval_len=interval_len, interval_type=interval_type)


class InfluxDBDeltaRequest(InfluxDBValueRequest, metaclass=events.GlobalRegister):
    def __init__(self, client: DataFrameClient, interval_len: int, interval_type: str = 's'):
        super().__init__(value='close - open as delta, period_volume, total_volume', client=client, interval_len=interval_len, interval_type=interval_type)


def _query_where(interval_len: int, interval_type: str, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None):
    """
    generate query where string
    :param interval_len: interval length
    :param interval_type: interval type
    :param symbol: symbol or symbol list
    :param bgn_prd: start datetime (excluding)
    :param end_prd: end datetime (excluding)
    :return: data from the database
    """

    result = " WHERE" \
             " interval = '{}'" + \
             ('' if symbol is None else " AND symbol =" + ("~ /{}/ " if isinstance(symbol, list) else " '{}'")) + \
             ('' if bgn_prd is None else " AND time > '{}'") + \
             ('' if end_prd is None else " AND time < '{}'")

    args = tuple(filter(lambda x: x is not None, [str(interval_len) + '_' + interval_type, None if symbol is None else "|".join(['^' + s + '$' for s in symbol]) if isinstance(symbol, list) else symbol, bgn_prd, end_prd]))
    return result.format(*args)
