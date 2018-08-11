import datetime
import functools
import json
import typing

import pandas as pd
from dateutil import tz

from atpy.data.cache.postgres_cache import insert_json
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsInPeriodFilter, BarsDailyForDatesFilter


def noncache_provider(history: IQFeedHistoryProvider):
    def _request_noncache_data(filters: dict, q, h: IQFeedHistoryProvider):
        """
        :return: request data from data provider (has to be UTC localized)
        """
        new_filters = list()
        filters_copy = filters.copy()

        for f in filters_copy:
            if f.interval_type == 's':
                new_f = BarsInPeriodFilter(ticker=f.ticker, bgn_prd=f.bgn_prd.astimezone(tz.gettz('US/Eastern')) if f.bgn_prd is not None else None, end_prd=None, interval_len=f.interval_len, interval_type=f.interval_type)
            elif f.interval_type == 'd':
                new_f = BarsDailyForDatesFilter(ticker=f.ticker, bgn_dt=f.bgn_prd.date() if f.bgn_prd is not None else None, end_dt=None)

            filters[new_f] = f

            new_filters.append(new_f)

        h.request_data_by_filters(new_filters, q)

    return functools.partial(_request_noncache_data, h=history)


def update_fundamentals(conn, fundamentals: dict, table_name: str = 'json_data'):
    to_store = list()
    for v in fundamentals.values():
        v['provider'] = 'iqfeed'
        v['type'] = 'fundamentals'

        to_store.append(json.dumps(v, default=lambda x: x.isoformat() if isinstance(x, datetime.datetime) else str(x)))

    insert_json(conn=conn, table_name=table_name, data='\n'.join(to_store))


def request_fundamentals(conn, symbol: typing.Union[list, str], table_name: str = 'json_data'):
    where = " WHERE json_data ->> 'type' = 'fundamentals' AND json_data ->> 'provider' = 'iqfeed'"
    params = list()

    if isinstance(symbol, list):
        where += " AND json_data ->> 'symbol' IN (%s)" % ','.join(['%s'] * len(symbol))
        params += symbol
    elif isinstance(symbol, str):
        where += " AND json_data ->> 'symbol' = %s"
        params.append(symbol)

    cursor = conn.cursor()
    cursor.execute("select * from {0} {1}".format(table_name, where), params)
    records = cursor.fetchall()

    if len(records) > 0:
        df = pd.DataFrame([x[0] for x in records]).drop(['type', 'provider'], axis=1)

    return df
