import datetime
import json
import typing

import pandas as pd
from influxdb import DataFrameClient, InfluxDBClient

from atpy.data.cache.influxdb_cache_requests import InfluxDBOHLCRequest, get_adjustments
from atpy.data.util import adjust


class IQFeedInfluxDBOHLCRequest(InfluxDBOHLCRequest):

    def __init__(self, client: DataFrameClient, interval_len: int, interval_type: str = 's', adjust_data: bool = True):
        super().__init__(client=client, interval_len=interval_len, interval_type=interval_type)
        self.adjust_data = adjust_data

    def _request_raw_data(self, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, ascending: bool = True):
        result = super()._request_raw_data(symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd, ascending=ascending)
        if self.adjust_data:
            if isinstance(result.index, pd.MultiIndex):
                adjustments = get_adjustments(self.client, symbol=symbol, data_provider='iqfeed')

                def adj(x):
                    if x.name in adjustments:
                        adjust(x.name, x, adjustments=adjustments[x.name])

                result.update(result.groupby(level=0).apply(adj))
            elif isinstance(symbol, str):
                adjust(symbol, result, get_adjustments(self.client, symbol, data_provider='iqfeed'))

        return result


def get_cache_fundamentals(client: InfluxDBClient, symbol: typing.Union[list, str]=None):
    query = "SELECT * FROM iqfeed_fundamentals"
    if isinstance(symbol, list) and len(symbol) > 0:
        query += " WHERE symbol =~ /{}/".format("|".join(['^' + s + '$' for s in symbol]))
    elif isinstance(symbol, str) and len(symbol) > 0:
        query += " WHERE symbol = '{}'".format(symbol)

    result = {f['symbol']: {**json.loads(f['data']), **{'last_update': f['time']}} for f in list(InfluxDBClient.query(client, query, chunked=True).get_points())}

    return result[symbol] if isinstance(symbol, str) else result
