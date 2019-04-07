import datetime
import logging
import queue
import threading
import typing
from functools import partial

from dateutil import tz
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from influxdb import InfluxDBClient, DataFrameClient


class BarsFilter(typing.NamedTuple):
    ticker: typing.Union[list, str]
    interval_len: int
    interval_type: str
    bgn_prd: datetime.datetime


def ranges(client: InfluxDBClient):
    """
    :return: list of latest times for each entry grouped by symbol and interval
    """
    parse_time = lambda t: parse(t).replace(tzinfo=tz.gettz('UTC'))

    points = InfluxDBClient.query(client, "select FIRST(close), symbol, interval, time from bars group by symbol, interval").get_points()
    firsts = {(entry['symbol'], int(entry['interval'].split('_')[0]), entry['interval'].split('_')[1]): parse_time(entry['time']) for entry in points}

    points = InfluxDBClient.query(client, "select LAST(close), symbol, interval, time from bars group by symbol, interval").get_points()
    lasts = {(entry['symbol'], int(entry['interval'].split('_')[0]), entry['interval'].split('_')[1]): parse_time(entry['time']) for entry in points}

    result = {k: (firsts[k], lasts[k]) for k in firsts.keys() & lasts.keys()}

    return result


def update_to_latest(client: DataFrameClient, noncache_provider: typing.Callable, new_symbols: set = None, time_delta_back: relativedelta = relativedelta(years=5), skip_if_older_than: relativedelta = None):
    """
    Update existing entries in the database to the most current values
    :param client: DataFrameClient client
    :param noncache_provider: Non cache data provider
    :param new_symbols: additional symbols to add {(symbol, interval_len, interval_type), ...}}
    :param time_delta_back: start
    :param skip_if_older_than: skip symbol update if the symbol is older than...
    :return:
    """
    filters = dict()

    new_symbols = set() if new_symbols is None else new_symbols

    if skip_if_older_than is not None:
        skip_if_older_than = (datetime.datetime.utcnow().replace(tzinfo=tz.gettz('UTC')) - skip_if_older_than).astimezone(tz.gettz('US/Eastern'))

    for key, time in [(e[0], e[1][1]) for e in ranges(client).items()]:
        if key in new_symbols:
            new_symbols.remove(key)

        if skip_if_older_than is None or time > skip_if_older_than:
            bgn_prd = datetime.datetime.combine(time.date(), datetime.datetime.min.time()).replace(tzinfo=tz.gettz('US/Eastern'))
            filters[BarsFilter(ticker=key[0], bgn_prd=bgn_prd, interval_len=key[1], interval_type=key[2])] = None

    bgn_prd = datetime.datetime.combine(datetime.datetime.utcnow().date() - time_delta_back, datetime.datetime.min.time()).replace(tzinfo=tz.gettz('US/Eastern'))
    for (symbol, interval_len, interval_type) in new_symbols:
        filters[BarsFilter(ticker=symbol, bgn_prd=bgn_prd, interval_len=interval_len, interval_type=interval_type)] = None

    logging.getLogger(__name__).info("Updating " + str(len(filters)) + " total symbols and intervals; New symbols and intervals: " + str(len(new_symbols)))

    q = queue.Queue(maxsize=100)

    threading.Thread(target=partial(noncache_provider, filters=filters, q=q), daemon=True).start()

    try:
        for i, tupl in enumerate(iter(q.get, None)):
            ft, to_cache = filters[tupl[0]], tupl[1]

            if to_cache is not None and not to_cache.empty:
                # Prepare data
                for c in [c for c in to_cache.columns if c not in ['symbol', 'open', 'high', 'low', 'close', 'volume']]:
                    to_cache.drop(c, axis=1, inplace=True)

                to_cache['interval'] = str(ft.interval_len) + '_' + ft.interval_type

                if to_cache.iloc[0].name == ft.bgn_prd:
                    to_cache = to_cache.iloc[1:]

                try:
                    client.write_points(to_cache, 'bars', protocol='line', tag_columns=['symbol', 'interval'], time_precision='s')
                except Exception as err:
                    logging.getLogger(__name__).exception(err)

            if i > 0 and (i % 20 == 0 or i == len(filters)):
                logging.getLogger(__name__).info("Cached " + str(i) + " queries")
    finally:
        client.close()


def add_adjustments(client: InfluxDBClient, adjustments: list, provider: str):
    """
    add a list of splits/dividends to the database
    :param client: influxdb client
    :param adjustments: list of adjustments of the type [(timestamp: datetime.date, symbol: str, typ: str, value), ...]
    :param provider: data provider
    """
    points = [_get_adjustment_json_query(*a, provider=provider) for a in adjustments]
    return InfluxDBClient.write_points(client, points, protocol='json', time_precision='s')


def add_adjustment(client: InfluxDBClient, timestamp: datetime.date, symbol: str, typ: str, value: float, provider: str):
    """
    add splits/dividends to the database
    :param client: influxdb client
    :param timestamp: date of the adjustment
    :param symbol: symbol
    :param typ: 'split' or 'dividend'
    :param value: split_factor/dividend_rate
    :param provider: data provider
    """
    json_body = _get_adjustment_json_query(timestamp=timestamp, symbol=symbol, typ=typ, value=value, provider=provider)
    return InfluxDBClient.write_points(client, [json_body], protocol='json', time_precision='s')


def _get_adjustment_json_query(timestamp: datetime.date, symbol: str, typ: str, value: float, provider: str):
    return {
        "measurement": "splits_dividends",
        "tags": {
            "symbol": symbol,
            "provider": provider,
        },

        "time": datetime.datetime.combine(timestamp, datetime.datetime.min.time()),
        "fields": {'value': value, 'type': typ}
    }
