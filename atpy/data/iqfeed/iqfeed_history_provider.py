import datetime
import queue
import threading

import atpy.data.iqfeed.util as iqfeedutil
import pyiqfeed as iq
from atpy.data.iqfeed.filters import *
from pyevents.events import *


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


class IQFeedHistoryListener(object):
    """
    IQFeed historical data listener. See the unit test on how to use
    """

    def __init__(self, minibatch=None, column_mode=True, key_suffix='', filter_provider=DefaultFilterProvider()):
        """
        :param minibatch: size of the minibatch
        :param column_mode: whether to organize the data in columns or rows
        :param key_suffix: suffix for field names
        :param filter_provider: news filter list
        """
        self.minibatch = minibatch
        self.column_mode = column_mode
        self.key_suffix = key_suffix
        self.current_minibatch = list()
        self.filter_provider = filter_provider
        self.conn = None

    def __enter__(self):
        iqfeedutil.launch_service()
        self.conn = iq.HistoryConn()
        self.conn.connect()
        self.is_running = True
        self.producer_thread = threading.Thread(target=self.produce, daemon=True)
        self.producer_thread.start()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.conn.disconnect()
        self.conn = None
        self.is_running = False

    def __del__(self):
        if self.conn is not None:
            self.conn.disconnect()
            self.cfg = None

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

            processed_data = list()

            for datum in data:
                processed_data.append(datum[0] if len(datum) == 1 else datum)

                if self.minibatch is not None:
                    self.current_minibatch.append(datum[0] if len(datum) == 1 else datum)

                    if len(self.current_minibatch) == self.minibatch:
                        self.process_minibatch(self.current_minibatch)
                        self.current_minibatch = list()

            self.process_batch(processed_data)

            if not self.is_running:
                return

    @after
    def process_batch(self, data):
        return iqfeedutil.create_batch(data, self.column_mode, self.key_suffix)

    @after
    def process_minibatch(self, data):
        return iqfeedutil.create_batch(data, self.column_mode, self.key_suffix)


class IQFeedHistoryProvider(IQFeedHistoryListener):
    """
    IQFeed historical data provider (not streaming). See the unit test on how to use
    """

    def __init__(self, minibatch=1, column_mode=True, key_suffix='', filter_provider=DefaultFilterProvider(), use_minibatch=True):
        """
        :param minibatch: size of the minibatch
        :param column_mode: whether to organize the data in columns or rows
        :param key_suffix: suffix for field names
        :param filter_provider: news filter list
        """
        super().__init__(minibatch=minibatch, key_suffix=key_suffix, column_mode=column_mode, filter_provider=filter_provider)

        if use_minibatch:
            self.process_minibatch += lambda *args, **kwargs: self.queue.put(kwargs[FUNCTION_OUTPUT])
        else:
            self.process_batch += lambda *args, **kwargs: self.queue.put(kwargs[FUNCTION_OUTPUT])

    def __enter__(self):
        self.queue = queue.Queue()
        super().__enter__()
        return self

    def __iter__(self):
        return self

    def __next__(self) -> map:
        return self.queue.get()
