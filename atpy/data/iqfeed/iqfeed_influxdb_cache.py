import datetime
import json
import logging

from dateutil import tz
from dateutil.relativedelta import relativedelta

from atpy.data.cache.influxdb_cache import InfluxDBCache, ClientFactory
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsInPeriodFilter


class IQFeedInfluxDBCache(InfluxDBCache):
    """
    InfluxDB bar data cache using IQFeed data provider
    """

    def __init__(self, client_factory: ClientFactory, listeners=None, history: IQFeedHistoryProvider = None, use_stream_events=True, time_delta_back: relativedelta = relativedelta(years=5)):
        super().__init__(client_factory=client_factory, listeners=listeners, use_stream_events=use_stream_events, time_delta_back=time_delta_back)
        self.history = history

    def _request_noncache_datum(self, symbol, bgn_prd, interval_len, interval_type='s'):
        if bgn_prd is not None:
            bgn_prd = bgn_prd.astimezone(tz.gettz('US/Eastern'))

        f = BarsInPeriodFilter(ticker=symbol, bgn_prd=bgn_prd, end_prd=None, interval_len=interval_len, interval_type=interval_type)
        return self.history.request_data(f, adjust_data=False)

    def _request_noncache_data(self, filters, q):
        new_filters = list()
        for f in filters:
            if f.bgn_prd is not None:
                new_filters.append(BarsInPeriodFilter(ticker=f.ticker, bgn_prd=f.bgn_prd.astimezone(tz.gettz('US/Eastern')), end_prd=None, interval_len=f.interval_len, interval_type=f.interval_type))
            else:
                new_filters.append(BarsInPeriodFilter(ticker=f.ticker, bgn_prd=f.bgn_prd, end_prd=None, interval_len=f.interval_len, interval_type=f.interval_type))

        self.history.request_data_by_filters(new_filters, q, adjust_data=False)

    def update_fundamentals(self, fundamentals: list):
        client = self.client_factory.new_client()
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
            client.write_points(points, protocol='json', time_precision='s')
        except Exception as err:
            logging.getLogger(__name__).error(err)

    def update_splits_dividends(self, fundamentals: list):
        points = list()
        for f in fundamentals:
            if f['split_factor_1_date'] is not None and f['split_factor_1'] is not None:
                points.append((f['split_factor_1_date'], f['symbol'], 'split', f['split_factor_1']))

            if f['split_factor_2_date'] is not None and f['split_factor_2'] is not None:
                points.append((f['split_factor_2_date'], f['symbol'], 'split', f['split_factor_2']))

            if f['ex-dividend_date'] is not None and f['dividend_amount'] is not None:
                points.append((f['ex-dividend_date'], f['symbol'], 'dividend', f['dividend_amount']))

        self.add_adjustments(points, 'iqfeed')
