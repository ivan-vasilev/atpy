import datetime
import typing

import numpy as np
from influxdb import DataFrameClient

import pyevents.events as events


class InfluxDBOHLCRequest(object, metaclass=events.GlobalRegister):
    def __init__(self, client: DataFrameClient, default_timezone: str = 'US/Eastern'):
        self.client = client
        self._default_timezone = default_timezone

    @events.listener
    def on_event(self, event):
        if event['type'] == 'request_ohlc':
            self.request_result(self.request(**event['data']))

    @events.after
    def request_result(self, data):
        return {'type': 'cache_result', 'data': data}

    def request(self, interval_len: int, interval_type: str, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, ascending: bool = True):
        """
        :param interval_len: interval length
        :param interval_type: interval type
        :param symbol: symbol or symbol list
        :param bgn_prd: start datetime (excluding)
        :param end_prd: end datetime (excluding)
        :param ascending: asc/desc
        :return: data from the database
        """

        query = "SELECT * FROM bars" + \
                _query_where(interval_len=interval_len, interval_type=interval_type, symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd) + \
                " ORDER BY time " + "ASC" if ascending else "DESC"

        result = self.client.query(query)
        if len(result) == 0:
            result = None
        else:
            result = result['bars']
            result.drop('interval', axis=1, inplace=True)
            result.index.name = 'timestamp'
            result = result[['open', 'high', 'low', 'close', 'total_volume', 'period_volume', 'number_of_trades', 'symbol']]

            if self._default_timezone is not None:
                result.index = result.index.tz_convert(self._default_timezone)

            for c in [c for c in result.columns if result[c].dtype == np.int64]:
                result[c] = result[c].astype(np.uint64, copy=False)

            result['timestamp'] = result.index

            if len(result['symbol'].unique()) > 1:
                result.set_index('symbol', drop=False, append=True, inplace=True)
                result = result.swaplevel(0, 1, axis=0)
                result.sort_index(inplace=True, ascending=ascending)

            result = result[['open', 'high', 'low', 'close', 'total_volume', 'period_volume', 'number_of_trades', 'timestamp', 'symbol']]

        return result


class InfluxDBDeltaRequest(object, metaclass=events.GlobalRegister):
    def __init__(self, client: DataFrameClient, default_timezone: str = 'US/Eastern'):
        self.client = client
        self._default_timezone = default_timezone

    @events.listener
    def on_event(self, event):
        if event['type'] == 'request_delta':
            self.request_result(self.request(**event['data']))

    @events.after
    def request_result(self, data):
        return {'type': 'cache_result', 'data': data}

    def request(self, interval_len: int, interval_type: str, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, ascending: bool = True):
        """
        :param interval_len: interval length
        :param interval_type: interval type
        :param symbol: symbol or symbol list
        :param bgn_prd: start datetime (excluding)
        :param end_prd: end datetime (excluding)
        :param ascending: asc/desc
        :return: data from the database
        """

        query = "SELECT symbol, (close - open) / open as delta FROM bars" + \
                _query_where(interval_len=interval_len, interval_type=interval_type, symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd) + \
                " ORDER BY time " + "ASC" if ascending else "DESC"

        result = self.client.query(query)
        if len(result) == 0:
            result = None
        else:
            result = result['bars']
            result.index.name = 'timestamp'

            if self._default_timezone is not None:
                result.index = result.index.tz_convert(self._default_timezone)

            for c in [c for c in result.columns if result[c].dtype == np.int64]:
                result[c] = result[c].astype(np.uint64, copy=False)

            result['timestamp'] = result.index

            if len(result['symbol'].unique()) > 1:
                result.set_index('symbol', drop=False, append=True, inplace=True)
                result = result.swaplevel(0, 1, axis=0)
                result.sort_index(inplace=True, ascending=ascending)

        return result


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

    args = tuple(filter(lambda x: x is not None, [str(interval_len) + '_' + interval_type, None if symbol is None else "|".join(symbol) if isinstance(symbol, list) else symbol, bgn_prd, end_prd]))
    return result.format(*args)
