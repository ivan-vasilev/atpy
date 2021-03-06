import datetime
import functools
import json
import logging

from dateutil import tz
from influxdb import InfluxDBClient

from atpy.data.cache.influxdb_cache import add_adjustments
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsInPeriodFilter, BarsDailyForDatesFilter


def noncache_provider(history: IQFeedHistoryProvider):
    def _request_noncache_data(filters, q, h: IQFeedHistoryProvider):
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


def update_fundamentals(client: InfluxDBClient, fundamentals: list):
    points = list()
    for f in fundamentals:
        points.append(
            {
                "measurement": "iqfeed_fundamentals",
                "tags": {
                    "symbol": f['symbol'],
                },
                "time": datetime.datetime.combine(datetime.datetime.utcnow().date(), datetime.datetime.min.time()),
                "fields": {
                    "data": json.dumps(f, default=lambda x: x.isoformat() if isinstance(x, datetime.datetime) else str(x)),
                }
            }
        )

    try:
        InfluxDBClient.write_points(client, points, protocol='json', time_precision='s')
    except Exception as err:
        logging.getLogger(__name__).error(err)


def update_splits_dividends(client: InfluxDBClient, fundamentals: list):
    points = list()
    for f in fundamentals:
        if f['split_factor_1_date'] is not None and f['split_factor_1'] is not None:
            points.append((f['split_factor_1_date'], f['symbol'], 'split', f['split_factor_1']))

        if f['split_factor_2_date'] is not None and f['split_factor_2'] is not None:
            points.append((f['split_factor_2_date'], f['symbol'], 'split', f['split_factor_2']))

        if f['ex-dividend_date'] is not None and f['dividend_amount'] is not None:
            points.append((f['ex-dividend_date'], f['symbol'], 'dividend', f['dividend_amount']))

    add_adjustments(client=client, adjustments=points, provider='iqfeed')
