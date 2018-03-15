import json
import typing

from influxdb import InfluxDBClient


def get_cache_fundamentals(client: InfluxDBClient, symbol: typing.Union[list, str] = None):
    query = "SELECT * FROM iqfeed_fundamentals"
    if isinstance(symbol, list) and len(symbol) > 0:
        query += " WHERE symbol =~ /{}/".format("|".join(['^' + s + '$' for s in symbol]))
    elif isinstance(symbol, str) and len(symbol) > 0:
        query += " WHERE symbol = '{}'".format(symbol)

    result = {f['symbol']: {**json.loads(f['data']), **{'last_update': f['time']}} for f in list(InfluxDBClient.query(client, query, chunked=True).get_points())}

    return result[symbol] if isinstance(symbol, str) else result
