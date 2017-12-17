from dateutil.relativedelta import relativedelta
from influxdb import DataFrameClient

from atpy.data.cache.influxdb_cache import InfluxDBCache
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsInPeriodFilter


class IQFeedInfluxDBCache(InfluxDBCache):
    """
    InfluxDB bar data cache using IQFeed data provider
    """

    def __init__(self, client: DataFrameClient, history: IQFeedHistoryProvider=None, use_stream_events=True, time_delta_back: relativedelta = relativedelta(years=5), default_timezone: str = 'US/Eastern'):
        super().__init__(client=client, use_stream_events=use_stream_events, time_delta_back=time_delta_back, default_timezone=default_timezone)
        self._history = history

    def __enter__(self):
        self.own_history = self.history is None
        if self.own_history:
            self.history = IQFeedHistoryProvider(exclude_nan_ratio=None)
            self.history.__enter__()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.own_history:
            self.history.__exit__(exception_type, exception_value, traceback)

    @property
    def history(self):
        return self._history

    @history.setter
    def history(self, x):
        self._history = x

    def _request_noncache_datum(self, symbol, bgn_prd, interval_len, interval_type='s'):
        f = BarsInPeriodFilter(ticker=symbol, bgn_prd=bgn_prd, end_prd=None, interval_len=interval_len, interval_type=interval_type)
        return self.history.request_data(f, synchronize_timestamps=False, adjust_data=False)

    def _request_noncache_data(self, filters, q):
        new_filters = [BarsInPeriodFilter(ticker=f.ticker, bgn_prd=f.bgn_prd, end_prd=None, interval_len=f.interval_len, interval_type=f.interval_type) for f in filters]
        self.history.request_data_by_filters(new_filters, q, adjust_data=False)
