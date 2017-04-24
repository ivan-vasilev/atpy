import datetime
import pickle
import typing

import lmdb

import atpy.data.iqfeed.util as iqfeedutil
import pyevents.events as events
import pyiqfeed
from atpy.data.iqfeed.filters import *
from atpy.data.iqfeed.iqfeed_level_1_provider import Fundamentals
from atpy.data.iqfeed.util import *
from multiprocessing.pool import ThreadPool
import logging


class TicksFilter(NamedTuple):
    """
    Ticks filter parameters
    """

    ticker: typing.Union[list, str]
    max_ticks: int
    ascend: bool
    timeout: int

TicksFilter.__new__.__defaults__ = (False, None)


class TicksForDaysFilter(NamedTuple):
    """
    Ticks for days filter parameters
    """

    ticker: typing.Union[list, str]
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

    ticker: typing.Union[list, str]
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

    ticker: typing.Union[list, str]
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

    ticker: typing.Union[list, str]
    interval_len: int
    interval_type: str
    days: int
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

    ticker: typing.Union[list, str]
    interval_len: int
    interval_type: str
    bgn_prd: datetime.datetime
    end_prd: datetime.datetime
    bgn_flt: datetime.time
    end_flt: datetime.time
    ascend: bool
    max_ticks: int
    timeout: int

BarsInPeriodFilter.__new__.__defaults__ = (None, None, False, None, None)


class BarsDailyFilter(NamedTuple):
    """
    Daily bars filter parameters
    """

    ticker: typing.Union[list, str]
    num_days: int
    ascend: bool = False
    timeout: int = None

BarsDailyFilter.__new__.__defaults__ = (False, None)


class BarsDailyForDatesFilter(NamedTuple):
    """
    Daily bars for dates filter parameters
    """

    ticker: typing.Union[list, str]
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

    ticker: typing.Union[list, str]
    num_weeks: int
    ascend: bool
    timeout: int

BarsWeeklyFilter.__new__.__defaults__ = (False, None)


class BarsMonthlyFilter(NamedTuple):
    """
    Monthly bars filter parameters
    """

    ticker: typing.Union[list, str]
    num_months: int
    ascend: bool
    timeout: int

BarsMonthlyFilter.__new__.__defaults__ = (False, None)


class IQFeedHistoryListener(object, metaclass=events.GlobalRegister):
    """
    IQFeed historical data listener. See the unit test on how to use
    """

    def __init__(self, minibatch=None, fire_batches=False, fire_ticks=False, run_async=True, num_connections=10, key_suffix='', filter_provider=DefaultFilterProvider(), lmdb_path=None):
        """
        :param minibatch: size of the minibatch
        :param fire_batches: raise event for each batch
        :param fire_ticks: raise event for each tick
        :param run_async: run asynchronous
        :param num_connections: number of connections to use when requesting data
        :param key_suffix: suffix for field names
        :param filter_provider: news filter list
        :param lmdb_path: path to lmdb database. If not None, then the data is cached
        """
        self.minibatch = minibatch
        self.fire_batches = fire_batches
        self.fire_ticks = fire_ticks
        self.run_async = run_async
        self.num_connections = num_connections
        self.key_suffix = key_suffix
        self.current_minibatch = None
        self.current_batch = None
        self.current_filter = None
        self.filter_provider = filter_provider
        self.fundamentals = dict()

        self.db = lmdb.open(lmdb_path) if lmdb_path is not None else None

        self.conn = None
        self.streaming_conn = None

    def __enter__(self):
        iqfeedutil.launch_service()

        if self.num_connections == 1:
            self.conn = iq.HistoryConn()
            self.conn.connect()
        else:
            self.conn = [iq.HistoryConn() for i in range(self.num_connections)]
            for c in self.conn:
                c.connect()

        # streaming conn for fundamental data
        self.streaming_conn = iq.QuoteConn()
        self.streaming_conn.connect()

        if self.run_async:
            self.producer_thread = threading.Thread(target=self.produce_async, daemon=True)
            self.producer_thread.start()

            self.is_running = True

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.is_running = False

        if isinstance(self.conn, list):
            for c in self.conn:
                c.disconnect()
        else:
            self.conn.disconnect()

        self.conn = None

        self.streaming_conn.disconnect()
        self.streaming_conn = None

    def __del__(self):
        if self.conn is not None:
            if isinstance(self.conn, list):
                for c in self.conn:
                    c.disconnect()
            else:
                self.conn.disconnect()

            self.conn = None

        if self.streaming_conn is not None:
            self.streaming_conn.disconnect()

    def produce_async(self):
        for f in self.filter_provider:
            try:
                if f is not None:
                    if isinstance(f.ticker, str):
                        self._produce_signal(f)
                    elif isinstance(f.ticker, list):
                        self._produce_signals(f)
                else:
                    self.is_running = False
            except Exception as err:
                logging.getLogger(__name__).exception(err)
                self.is_running = False

            if not self.is_running:
                return

    def produce(self):
        for f in self.filter_provider:
            if f is not None:
                if isinstance(f.ticker, str):
                    self._produce_signal(f)
                elif isinstance(f.ticker, list):
                    self._produce_signals(f)
            else:
                return

    def _produce_signal(self, f):
        data = self._request_data(f, self.conn[0] if isinstance(self.conn, list) else self.conn)
        if data is None:
            return

        batch = self._process_data(data, f)

        event_type = self._event_type(f)

        if self.fire_ticks:
            for i in range(batch.shape[0]):
                self.process_datum({'type': event_type, 'data': batch.iloc[i]})

        if self.minibatch is not None:
            if self.current_minibatch is None or (self.current_filter is not None and type(self.current_filter) != type(f)):
                self.current_minibatch = batch
            else:
                self.current_minibatch = pd.concat([self.current_minibatch, batch], axis=0)

            for i in range(self.minibatch, self.current_minibatch.shape[0] - self.current_minibatch.shape[0] % self.minibatch + 1, self.minibatch):
                self.process_minibatch({'type': event_type + '_mb', 'data': self.current_minibatch.iloc[i - self.minibatch: i]})

            self.current_minibatch = self.current_minibatch.iloc[i:]

        if self.fire_batches:
            self.process_batch({'type': event_type + '_batch', 'data': batch})

        self.current_filter = f
        self.current_batch = batch

    def _produce_signals(self, f):
        signals = dict()

        if self.num_connections > 0:
            pool = ThreadPool(self.num_connections)

            def mp_worker(p):
                try:
                    sig, ft, conn = p
                    data = self._request_data(ft, conn)
                    if data is not None:
                        sig[ft.ticker] = self._process_data(data, ft)
                except Exception as err:
                    logging.getLogger(__name__).exception(err)
                    raise err

            pool.map(mp_worker, ((signals, f._replace(ticker=t), self.conn[i % self.num_connections]) for i, t in enumerate(f.ticker)))
            pool.close()
            pool.join()
        else:
            for ft in [f._replace(ticker=t) for t in f.ticker]:
                data = self._request_data(ft)
                if data is not None:
                    signals[ft.ticker] = self._process_data(data, ft)

        if len(signals) > 0:
            col = 'Time Stamp' if 'Time Stamp' in list(signals.values())[0] else 'Date' if 'Date' in list(signals.values())[0] else None
            if col is not None:
                ts = pd.Series(name=col)
                for _, s in signals.items():
                    s.drop_duplicates(subset=col, keep='last', inplace=True)
                    ts = ts.append(s[col])

                ts = ts.drop_duplicates()
                ts.sort_values(inplace=True)
                ts = ts.to_frame()

                for symbol, signal in signals.items():
                    signals[symbol] = pd.merge_ordered(signal, ts, on=col, how='outer', fill_method='ffill')

                    if self.current_filter is not None and type(self.current_filter) == type(f) and self.current_batch is not None and symbol in self.current_batch:
                        signals[symbol].fillna(value=self.current_batch[symbol, self.current_batch.shape[1] - 1], inplace=True)
                    else:
                        signals[symbol].fillna(method='backfill', inplace=True)

            batch = pd.Panel.from_dict(signals)

            event_type = self._event_type(f)

            if self.fire_ticks:
                for i in range(batch.shape[1]):
                    self.process_datum({'type': event_type, 'data': batch.iloc[:, i]})

            if self.minibatch is not None:
                if self.current_minibatch is None or (self.current_filter is not None and type(self.current_filter) != type(f)):
                    self.current_minibatch = batch.copy(deep=True)
                else:
                    self.current_minibatch = pd.concat([self.current_minibatch, batch], axis=1)

                for i in range(self.minibatch, self.current_minibatch.shape[1] - self.current_minibatch.shape[1] % self.minibatch + 1, self.minibatch):
                    self.process_minibatch({'type': event_type + '_mb', 'data': self.current_minibatch.iloc[: i - self.minibatch: i]})

                self.current_minibatch = self.current_minibatch.iloc[:, i:]

            if self.fire_batches:
                self.process_batch({'type': event_type + '_batch', 'data': batch})

            self.current_filter = f
            self.current_batch = batch

    def _request_data(self, f, conn):
        adjust_data = False

        if isinstance(f, TicksFilter):
            method = conn.request_ticks
            adjust_data = True
        elif isinstance(f, TicksForDaysFilter):
            method = conn.request_ticks_for_days
            adjust_data = True
        elif isinstance(f, TicksInPeriodFilter):
            method = conn.request_ticks_in_period
            adjust_data = True
        elif isinstance(f, BarsFilter):
            method = conn.request_bars
            adjust_data = True
        elif isinstance(f, BarsForDaysFilter):
            method = conn.request_bars_for_days
        elif isinstance(f, BarsInPeriodFilter):
            method = conn.request_bars_in_period
            adjust_data = True
        elif isinstance(f, BarsDailyFilter):
            method = conn.request_daily_data
        elif isinstance(f, BarsDailyForDatesFilter):
            method = conn.request_daily_data_for_dates
        elif isinstance(f, BarsWeeklyFilter):
            method = conn.request_weekly_data
        elif isinstance(f, BarsMonthlyFilter):
            method = conn.request_monthly_data

        try:
            if self.db is not None:
                with self.db.begin() as txn:
                    data = txn.get(bytearray(f.__str__(), encoding='ascii'))

                if data is None:
                    data = method(*f)

                    if data is not None:
                        with self.db.begin(write=True) as txn:
                            txn.put(bytearray(f.__str__(), encoding='ascii'), pickle.dumps(data))
                else:
                    data = pickle.loads(data)
            else:
                data = method(*f)

            if adjust_data and data is not None:
                adjust(data, Fundamentals.get(f.ticker, self.streaming_conn))
        except pyiqfeed.exceptions.NoDataError:
            return None

        return data

    def _process_data(self, data, data_filter):
        if isinstance(data_filter, TicksFilter) or isinstance(data_filter, TicksForDaysFilter) or isinstance(data_filter, TicksInPeriodFilter):
            return self._process_ticks_data(data, data_filter)
        elif isinstance(data_filter, BarsFilter) or isinstance(data_filter, BarsForDaysFilter) or isinstance(data_filter, BarsInPeriodFilter):
            return self._process_bars_data(data, data_filter)
        elif isinstance(data_filter, BarsDailyFilter) or isinstance(data_filter, BarsDailyForDatesFilter) or isinstance(data_filter, BarsWeeklyFilter) or isinstance(data_filter, BarsMonthlyFilter):
            return self._process_daily_data(data, data_filter)

    def _process_ticks_data(self, data, data_filter):
        result = pd.DataFrame(data)
        sf = self.key_suffix
        result['Time Stamp' + sf] = data['date'] + data['time']
        result.drop(['date', 'time'], axis=1, inplace=True)
        result.rename_axis({"last": "Last" + sf, "last_sz": "Last Size" + sf, "tot_vlm": "Total Volume" + sf, "bid": "Bid" + sf, "ask": "Ask" + sf, "tick_id": "TickID" + sf, "last_type": "Basis For Last" + sf, "mkt_ctr": "Trade Market Center" + sf}, axis="columns", copy=False, inplace=True)
        result['Symbol'] = data_filter.ticker

        return result

    def _process_bars_data(self, data, data_filter):
        result = pd.DataFrame(data)
        sf = self.key_suffix
        result['Time Stamp' + sf] = data['date'] + data['time']
        result.drop(['date', 'time'], axis=1, inplace=True)
        result.rename_axis({"high_p": "High" + sf, "low_p": "Low" + sf, "open_p": "Open" + sf, "close_p": "Close" + sf, "tot_vlm": "Total Volume" + sf, "prd_vlm": "Period Volume" + sf, "num_trds": "Number of Trades" + sf}, axis="columns", copy=False, inplace=True)
        result['Symbol'] = data_filter.ticker

        return result

    def _process_daily_data(self, data, data_filter):
        result = pd.DataFrame(data)
        sf = self.key_suffix
        result.rename_axis({"date": "Date" + sf, "high_p": "High" + sf, "low_p": "Low" + sf, "open_p": "Open" + sf, "close_p": "Close" + sf, "prd_vlm": "Period Volume" + sf, "open_int": "Open Interest" + sf}, axis="columns", copy=False, inplace=True)
        result['Symbol'] = data_filter.ticker

        return result

    @staticmethod
    def _event_type(data_filter):
        if isinstance(data_filter, TicksFilter) or isinstance(data_filter, TicksForDaysFilter) or isinstance(data_filter, TicksInPeriodFilter):
            return 'level_1_tick'
        elif isinstance(data_filter, BarsFilter) or isinstance(data_filter, BarsForDaysFilter) or isinstance(data_filter, BarsInPeriodFilter):
            return 'bar'
        elif isinstance(data_filter, BarsDailyFilter) or isinstance(data_filter, BarsDailyForDatesFilter) or isinstance(data_filter, BarsWeeklyFilter) or isinstance(data_filter, BarsMonthlyFilter):
            return 'daily'

    @events.after
    def process_datum(self, data):
        return data

    @events.after
    def process_batch(self, data):
        return data

    def batch_provider(self):
        return IQFeedDataProvider(self.process_batch)

    @events.after
    def process_minibatch(self, data):
        return data

    def minibatch_provider(self):
        return IQFeedDataProvider(self.process_minibatch)


class TicksInPeriodProvider(FilterProvider):
    """
    Generate a sequence of TicksInPeriod filters to obtain market history
    """

    def __init__(self, ticker: typing.Union[list, str], bgn_prd: datetime.datetime, delta: datetime.timedelta, bgn_flt: datetime.time=None, end_flt: datetime.time=None, ascend: bool=False, max_ticks: int=None, timeout: int=None):
        self.ticker = ticker
        self.bgn_prd = bgn_prd
        self.delta = delta
        self.bgn_flt = bgn_flt
        self.end_flt = end_flt
        self.ascend = ascend
        self.max_ticks = max_ticks
        self.timeout = timeout

    def __iter__(self):
        self._deltas = 0
        return self

    def __next__(self) -> NamedTuple:
        self._deltas += 1
        bgn_prd = self.bgn_prd + (self._deltas - 1) * self.delta
        now = datetime.datetime.now()

        if bgn_prd < now:
            end_prd = self.bgn_prd + self._deltas * self.delta
            end_prd = end_prd if end_prd < now else now
            return TicksInPeriodFilter(ticker=self.ticker, bgn_prd=bgn_prd, end_prd=end_prd, bgn_flt=self.bgn_flt, end_flt=self.end_flt, ascend=self.ascend, max_ticks=self.max_ticks, timeout=self.timeout)
        else:
            return None


class BarsInPeriodProvider(FilterProvider):
    """
    Generate a sequence of BarsInPeriod filters to obtain market history
    """

    def __init__(self, ticker: typing.Union[list, str], interval_len: int, interval_type: str, bgn_prd: datetime.datetime, delta: datetime.timedelta, bgn_flt: datetime.time=None, end_flt: datetime.time=None, ascend: bool=False, max_ticks: int=None, timeout: int=None):
        self.ticker = ticker
        self.interval_len = interval_len
        self.interval_type = interval_type
        self.bgn_prd = bgn_prd
        self.delta = delta
        self.bgn_flt = bgn_flt
        self.end_flt = end_flt
        self.ascend = ascend
        self.max_ticks = max_ticks
        self.timeout = timeout

    def __iter__(self):
        self._deltas = 0
        return self

    def __next__(self) -> NamedTuple:
        self._deltas += 1
        bgn_prd = self.bgn_prd + (self._deltas - 1) * self.delta
        now = datetime.datetime.now()

        if bgn_prd < now:
            end_prd = self.bgn_prd + self._deltas * self.delta
            end_prd = end_prd if end_prd < now else now

            return BarsInPeriodFilter(ticker=self.ticker, interval_len=self.interval_len, interval_type=self.interval_type, bgn_prd=bgn_prd, end_prd=end_prd, bgn_flt=self.bgn_flt, end_flt=self.end_flt, ascend=self.ascend, max_ticks=self.max_ticks, timeout=self.timeout)
        else:
            return None
