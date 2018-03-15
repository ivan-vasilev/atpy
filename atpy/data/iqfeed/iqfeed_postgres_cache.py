import functools

from dateutil import tz

from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsInPeriodFilter


def noncache_provider(history: IQFeedHistoryProvider):
    def _request_noncache_data(filters, q, h: IQFeedHistoryProvider):
        """
        :return: request data from data provider (has to be UTC localized)
        """
        new_filters = list()
        for f in filters:
            if f.bgn_prd is not None:
                new_filters.append(BarsInPeriodFilter(ticker=f.ticker, bgn_prd=f.bgn_prd.astimezone(tz.gettz('US/Eastern')), end_prd=None, interval_len=f.interval_len, interval_type=f.interval_type))
            else:
                new_filters.append(BarsInPeriodFilter(ticker=f.ticker, bgn_prd=f.bgn_prd, end_prd=None, interval_len=f.interval_len, interval_type=f.interval_type))

        h.request_data_by_filters(new_filters, q)

    return functools.partial(_request_noncache_data, h=history)

