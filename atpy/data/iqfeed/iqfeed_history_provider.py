from atpy.data.iqfeed.iqfeed_base_provider import *
from atpy.data.iqfeed.filters import *
import datetime
import numpy as np


class TicksFilter(NamedTuple):
    """
    Ticks filter parameters
    """

    ticker: str
    max_ticks: int
    ascend: bool
    timeout: int

TicksFilter.__new__.__defaults__ = (False, None)


class TicksForDaysFilter(NamedTuple):
    """
    Ticks for days filter parameters
    """

    ticker: str
    num_days: int
    bgn_flt: datetime.time
    end_flt: datetime.time
    ascend: bool
    max_ticks: int
    timeout: int

TicksForDaysFilter.__new__.__defaults__ = (None, None, False, None, None)


class TicksInPeriodFilter(NamedTuple):
    """
    Ticks in period filter parameters
    """

    ticker: str
    bgn_prd: datetime.datetime
    end_prd: datetime.datetime
    bgn_flt: datetime.time
    end_flt: datetime.time
    ascend: bool
    max_ticks: int
    timeout: int

TicksInPeriodFilter.__new__.__defaults__ = (None, None, False, None, None)


class BarsFilter(NamedTuple):
    """
    Bars filter parameters
    """

    ticker: str
    interval_len: int
    interval_type: str
    max_bars: int
    ascend: bool
    timeout: int

BarsFilter.__new__.__defaults__ = (False, None)


class BarsForDaysFilter(NamedTuple):
    """
    Bars for days filter parameters
    """

    ticker: str
    interval_len: int
    interval_type: str
    days: int
    num_days: int
    bgn_flt: datetime.time
    end_flt: datetime.time
    ascend: bool
    max_bars: int
    timeout: int

BarsForDaysFilter.__new__.__defaults__ = (None, None, False, None, None)


class BarsInPeriodFilter(NamedTuple):
    """
    Bars in period filter parameters
    """

    ticker: str
    interval_len: int
    interval_type: str
    bgn_prd: datetime.datetime
    end_prd: datetime.datetime
    bgn_flt: datetime.time
    end_flt: datetime.time
    ascend: bool
    max_ticks: int
    timeout: int

TicksInPeriodFilter.__new__.__defaults__ = (None, None, False, None, None)


class BarsDailyFilter(NamedTuple):
    """
    Daily bars filter parameters
    """

    ticker: str
    num_days: int
    ascend: bool = False
    timeout: int = None

BarsDailyFilter.__new__.__defaults__ = (False, None)


class BarsDailyForDatesFilter(NamedTuple):
    """
    Daily bars for dates filter parameters
    """

    ticker: str
    bgn_dt: datetime.date
    end_dt: datetime.date
    ascend: bool = False
    max_days: int = None
    timeout: int = None

BarsDailyForDatesFilter.__new__.__defaults__ = (False, None, None)


class BarsWeeklyFilter(NamedTuple):
    """
    Weekly bars filter parameters
    """

    ticker: str
    num_weeks: int
    ascend: bool
    timeout: int

BarsWeeklyFilter.__new__.__defaults__ = (False, None)


class BarsMonthlyFilter(NamedTuple):
    """
    Monthly bars filter parameters
    """

    ticker: str
    num_months: int
    ascend: bool
    timeout: int

BarsMonthlyFilter.__new__.__defaults__ = (False, None)


class IQFeedHistoryProvider(IQFeedBaseProvider):
    """
    IQFeed historical data provider (not streaming). See the unit test on how to use
    """

    def __init__(self, minibatch=1, key_suffix='', filter_provider=DefaultFilterProvider()):
        self.minibatch = minibatch
        self.conn = None
        self.key_suffix = key_suffix
        self.filter_provider = filter_provider

    def __iter__(self):
        super().__iter__()

        if self.conn is None:
            self.conn = iq.HistoryConn()
            self.conn.connect()

        return self

    def __enter__(self):
        super().__enter__()

        self.conn = iq.HistoryConn()
        self.conn.connect()

        self.queue = PCQueue(self.produce)
        self.queue.start()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.queue.stop()
        self.conn.disconnect()
        self.conn = None

    def __del__(self):
        if self.conn is not None:
            self.conn.disconnect()
            self.cfg = None

        if self.queue is not None:
            self.queue.stop()

    def __next__(self) -> map:
        for i, datum in enumerate(iter(self.queue.get, None)):
            if i == 0:
                result = {n: np.empty((self.minibatch,), d.dtype) for n, d in zip(datum.dtype.names, datum)}

            for j, f in enumerate(datum.dtype.names):
                result[f][i] = datum[j]

            if (i + 1) % self.minibatch == 0:
                return result

    def __getattr__(self, name):
        if self.conn is not None:
            return getattr(self.conn, name)
        else:
            raise AttributeError

    def produce(self):
        for f in self.filter_provider:
            if isinstance(f, TicksFilter):
                method = self.conn.request_ticks
            elif isinstance(f, TicksForDaysFilter):
                method = self.conn.request_ticks_for_days
            elif isinstance(f, TicksInPeriodFilter):
                method = self.conn.request_ticks_in_period
            elif isinstance(f, BarsFilter):
                method = self.conn.request_bars
            elif isinstance(f, BarsForDaysFilter):
                method = self.conn.request_bars_for_days
            elif isinstance(f, BarsInPeriodFilter):
                method = self.conn.request_bars_in_period
            elif isinstance(f, BarsDailyFilter):
                method = self.conn.request_daily_data
            elif isinstance(f, BarsDailyForDatesFilter):
                method = self.conn.request_daily_data_for_dates
            elif isinstance(f, BarsWeeklyFilter):
                method = self.conn.request_weekly_data
            elif isinstance(f, BarsMonthlyFilter):
                method = self.conn.request_monthly_data

            data = method(*f)

            for d in data:
                yield d
