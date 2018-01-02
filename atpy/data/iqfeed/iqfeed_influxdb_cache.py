from dateutil import tz
from dateutil.relativedelta import relativedelta

from atpy.data.cache.influxdb_cache import InfluxDBCache, ClientFactory
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsInPeriodFilter


class IQFeedInfluxDBCache(InfluxDBCache):
    """
    InfluxDB bar data cache using IQFeed data provider
    """

    def __init__(self, client_factory: ClientFactory, history: IQFeedHistoryProvider = None, use_stream_events=True, time_delta_back: relativedelta = relativedelta(years=5)):
        super().__init__(client_factory=client_factory, use_stream_events=use_stream_events, time_delta_back=time_delta_back)
        self._history = history

    def __enter__(self):
        super().__enter__()

        self.own_history = self.history is None
        if self.own_history:
            self.history = IQFeedHistoryProvider()
            self.history.__enter__()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        super().__exit__(exception_type, exception_value, traceback)

        if self.own_history:
            self.history.__exit__(exception_type, exception_value, traceback)

    @property
    def history(self):
        return self._history

    @history.setter
    def history(self, x):
        self._history = x

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
