import abc
import datetime
import logging
import os
import pickle
import typing
from multiprocessing.pool import ThreadPool

import lmdb
import pandas as pd

import atpy.data.iqfeed.util as iqfeedutil
import pyevents.events as events
import pyiqfeed
from atpy.data.iqfeed.filters import *
from atpy.data.iqfeed.iqfeed_level_1_provider import Fundamentals
from atpy.data.iqfeed.util import *


class TicksFilter(NamedTuple):
    """
    Ticks filter parameters
    """

    ticker: typing.Union[list, str]
    max_ticks: int
    ascend: bool
    timeout: int

TicksFilter.__new__.__defaults__ = (True, None)


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

TicksForDaysFilter.__new__.__defaults__ = (None, None, True, None, None)


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

TicksInPeriodFilter.__new__.__defaults__ = (None, None, True, None, None)


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

BarsFilter.__new__.__defaults__ = (True, None)


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

BarsForDaysFilter.__new__.__defaults__ = (None, None, True, None, None)


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

BarsInPeriodFilter.__new__.__defaults__ = (None, None, True, None, None)


class BarsDailyFilter(NamedTuple):
    """
    Daily bars filter parameters
    """

    ticker: typing.Union[list, str]
    num_days: int
    ascend: bool = False
    timeout: int = None

BarsDailyFilter.__new__.__defaults__ = (True, None)


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

BarsDailyForDatesFilter.__new__.__defaults__ = (True, None, None)


class BarsWeeklyFilter(NamedTuple):
    """
    Weekly bars filter parameters
    """

    ticker: typing.Union[list, str]
    num_weeks: int
    ascend: bool
    timeout: int

BarsWeeklyFilter.__new__.__defaults__ = (True, None)


class BarsMonthlyFilter(NamedTuple):
    """
    Monthly bars filter parameters
    """

    ticker: typing.Union[list, str]
    num_months: int
    ascend: bool
    timeout: int

BarsMonthlyFilter.__new__.__defaults__ = (True, None)


class IQFeedHistoryListener(object, metaclass=events.GlobalRegister):
    """
    IQFeed historical data listener. See the unit test on how to use
    """

    def __init__(self, minibatch=None, fire_batches=False, fire_ticks=False, run_async=True, num_connections=10, key_suffix='', filter_provider=None, lmdb_path=''):
        """
        :param minibatch: size of the minibatch
        :param fire_batches: raise event for each batch
        :param fire_ticks: raise event for each tick
        :param run_async: run asynchronous
        :param num_connections: number of connections to use when requesting data
        :param key_suffix: suffix for field names
        :param filter_provider: news filter list
        :param lmdb_path: '' to use default path, None, not to use lmdb
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
        self._is_running = False
        self._background_thread = None
        self.lmdb_path = lmdb_path
        self.conn = None
        self.streaming_conn = None

    def __enter__(self):
        if self.lmdb_path == '':
            self.db = lmdb.open(os.path.join(os.path.abspath('../' * (len(__name__.split('.')) - 2)), 'data', 'cache', 'history'), map_size=100000000000)
        elif self.lmdb_path is not None:
            self.db = lmdb.open(self.lmdb_path, map_size=100000000000)
        else:
            self.db = None

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

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self._background_thread is not None and self._background_thread.is_alive():
            self._is_running = False
            self._background_thread.join()
        else:
            self._is_running = False

        self._background_thread = None

        self.no_more_data()

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

    def start(self):
        if self.filter_provider is not None:
            if self.run_async:
                def produce_async():
                    try:
                        for d, f in self.next_batch():
                            self.fire_events(d, f)
                            if not self._is_running:
                                return
                    except Exception as err:
                        logging.getLogger(__name__).exception(err)
                        self._is_running = False

                    self._is_running = False
                    self.no_more_data()

                self._is_running = True
                self._background_thread = threading.Thread(target=produce_async, daemon=True)
                self._background_thread.start()
            else:
                for d, f in self.next_batch():
                    self.fire_events(d, f)

                self.no_more_data()

    def next_batch(self):
        for f in self.filter_provider:
            logging.getLogger(__name__).info("Loading data for filter " + str(f))

            d = self.request_data(f)

            self.current_filter = f
            self.current_batch = d

            yield d, f

        self.no_more_data()

    def stop(self):
        self._is_running = False

    def request_data(self, f, synchronize_timestamps=True, exclude_nan_ratio=0.9):
        """
        request history data
        :param f: filter tuple
        :param synchronize_timestamps: whether to synchronize timestamps between different signals
        :param exclude_nan_ratio: exclude stocks with more nans than the given ration (before synchronization)
        :return: 
        """
        if isinstance(f.ticker, str):
            data = self._request_raw_symbol_data(f, self.conn[0] if isinstance(self.conn, list) else self.conn)
            if data is None:
                logging.getLogger(__name__).warning("No data found for filter: " + str(f))
                return

            data = self._process_data(data, f)
            logging.getLogger(__name__).warning("Generated data: " + str(data))
            return data
        elif isinstance(f.ticker, list):
            signals = dict()

            if self.num_connections > 1:
                pool = ThreadPool(self.num_connections)
                self._global_counter = 0
                lock = threading.Lock()
                no_data = set()

                def mp_worker(p):
                    try:
                        ft, conn = p
                        raw_data = self._request_raw_symbol_data(ft, conn)
                    except Exception as err:
                        raw_data = None
                        logging.getLogger(__name__).exception(err)

                    if raw_data is not None:
                        signals[ft.ticker] = self._process_data(raw_data, ft)
                    else:
                        no_data.add(ft.ticker)

                    with lock:
                        self._global_counter += 1
                        if self._global_counter % 200 == 0 or self._global_counter == len(f.ticker):
                            logging.getLogger(__name__).info("Loaded " + str(self._global_counter) + " symbols")
                            if len(no_data) > 0:
                                logging.getLogger(__name__).info("No data found for " + str(len(no_data)) + " symbols: " + str(no_data))
                                no_data.clear()

                pool.map(mp_worker, ((f._replace(ticker=t), self.conn[i % self.num_connections]) for i, t in enumerate(f.ticker)))
                pool.close()
                pool.join()
                del self._global_counter
            else:
                for ft in [f._replace(ticker=t) for t in f.ticker]:
                    data = self._request_raw_symbol_data(ft, self.conn)
                    if data is not None:
                        signals[ft.ticker] = self._process_data(data, ft)

            if synchronize_timestamps:
                signals = self.synchronize_timestamps(signals, f, exclude_nan_ratio)

            return signals

    def synchronize_timestamps(self, signals: map, f: NamedTuple, exclude_nan_ratio=None):
        """
        synchronize timestamps between historical signals
        :param signals: map of dataframes for each equity
        :param f: filter tuple
        :param exclude_nan_ratio: exclude stocks with more nans than the given ration (before synchronization)
        :return: 
        """

        if len(signals) <= 1:
            return

        col = 'Time Stamp' if 'Time Stamp' in list(signals.values())[0] else 'Date' if 'Date' in list(signals.values())[0] else None
        if col is not None:
            ts = pd.Series(name=col)
            for _, s in signals.items():
                s.drop_duplicates(subset=col, keep='last', inplace=True)
                ts = ts.append(s[col])

            ts = ts.drop_duplicates()
            ts.sort_values(inplace=True)
            ts = ts.to_frame()

            if exclude_nan_ratio is not None:
                mean = np.mean(np.array([signal.shape[0] for signal in signals.values()]))
                signals = {symbol: signal for symbol, signal in signals.items() if signal.shape[0] >= mean * exclude_nan_ratio}

            zero_values = list()
            for symbol, signal in signals.items():
                if 0 in signal['Open'].values:
                    logging.getLogger(__name__).warning(symbol + " contains 0 in the Open column before timestamp sync")

                df = pd.merge_ordered(signal, ts, on=col, how='outer')

                for c in [c for c in ['Period Volume', 'Number of Trades'] if c in df.columns]:
                    df[c].fillna(0, inplace=True)

                if 'Open' in df.columns:
                    op = df['Open']

                    op.fillna(method='ffill', inplace=True)

                    if self.current_filter is not None and type(self.current_filter) == type(f) and self.current_batch is not None and f.ascend is True and symbol in self.current_batch:
                        op.fillna(value=self.current_batch[symbol, self.current_batch.shape[1] - 1]['Open'], inplace=True)
                    else:
                        op.fillna(method='backfill', inplace=True)

                    for c in [c for c in ['Close', 'High', 'Low'] if c in df.columns]:
                        df[c].fillna(op, inplace=True)

                df.fillna(method='ffill', inplace=True)

                if self.current_filter is not None and type(self.current_filter) == type(f) and self.current_batch is not None and f.ascend is True and symbol in self.current_batch:
                    df.fillna(value=self.current_batch[symbol, self.current_batch.shape[1] - 1], inplace=True)
                else:
                    df.fillna(method='backfill', inplace=True)

                if 0 in df['Open'].values:
                    logging.getLogger(__name__).warning(symbol + " contains 0 in the Open column after timestamp sync")
                    zero_values.append(symbol)

                if not f.ascend:
                    df = df.iloc[::-1]

                signals[symbol] = df

            for s in zero_values:
                del signals[s]

        result = pd.Panel.from_dict(signals)
        logging.getLogger(__name__).info("Generated data of shape: " + str(result.shape))
        return result

    def fire_events(self, data, f):
        event_type = self._event_type(f)

        if isinstance(data, pd.DataFrame):
            if self.fire_ticks:
                for i in range(data.shape[0]):
                    self.process_datum({'type': event_type, 'data': data.iloc[i]})

            if self.minibatch is not None:
                if self.current_minibatch is None or (self.current_filter is not None and type(self.current_filter) != type(f)):
                    self.current_minibatch = data
                else:
                    self.current_minibatch = pd.concat([self.current_minibatch, data], axis=0)

                for i in range(self.minibatch, self.current_minibatch.shape[0] - self.current_minibatch.shape[0] % self.minibatch + 1, self.minibatch):
                    self.process_minibatch({'type': event_type + '_mb', 'data': self.current_minibatch.iloc[i - self.minibatch: i]})

                self.current_minibatch = self.current_minibatch.iloc[i:]

            if self.fire_batches:
                self.process_batch({'type': event_type + '_batch', 'data': data})
        elif isinstance(data, pd.Panel):
            if self.fire_ticks:
                for i in range(data.shape[1]):
                    self.process_datum({'type': event_type, 'data': data.iloc[:, i]})

            if self.minibatch is not None:
                if self.current_minibatch is None or (self.current_filter is not None and type(self.current_filter) != type(f)):
                    self.current_minibatch = data.copy(deep=True)
                else:
                    self.current_minibatch = pd.concat([self.current_minibatch, data], axis=1)

                for i in range(self.minibatch, self.current_minibatch.shape[1] - self.current_minibatch.shape[1] % self.minibatch + 1, self.minibatch):
                    self.process_minibatch({'type': event_type + '_mb', 'data': self.current_minibatch.iloc[:, i - self.minibatch: i]})

                self.current_minibatch = self.current_minibatch.iloc[:, i:]

            if self.fire_batches:
                self.process_batch({'type': event_type + '_batch', 'data': data})

    def _request_raw_symbol_data(self, f, conn):
        adjust_data = False
        cache_data = False

        if isinstance(f, TicksFilter):
            method = conn.request_ticks
            adjust_data = True
        elif isinstance(f, TicksForDaysFilter):
            method = conn.request_ticks_for_days
            adjust_data = True
        elif isinstance(f, TicksInPeriodFilter):
            method = conn.request_ticks_in_period
            adjust_data = True
            cache_data = True
        elif isinstance(f, BarsFilter):
            method = conn.request_bars
            adjust_data = True
        elif isinstance(f, BarsForDaysFilter):
            method = conn.request_bars_for_days
        elif isinstance(f, BarsInPeriodFilter):
            method = conn.request_bars_in_period
            adjust_data = True
            cache_data = True
        elif isinstance(f, BarsDailyFilter):
            method = conn.request_daily_data
        elif isinstance(f, BarsDailyForDatesFilter):
            method = conn.request_daily_data_for_dates
            cache_data = True
        elif isinstance(f, BarsWeeklyFilter):
            method = conn.request_weekly_data
        elif isinstance(f, BarsMonthlyFilter):
            method = conn.request_monthly_data

        try:
            if cache_data and self.db is not None:
                with self.db.begin() as txn:
                    data = txn.get(bytearray(f.__str__(), encoding='ascii'))

                if data is None:
                    data = method(*f)

                    if data is not None:
                        with self.db.begin(write=True) as txn:
                            txn.put(bytearray(f.__str__(), encoding='ascii'), pickle.dumps(data))
                else:
                    data = pickle.loads(data)
                    if isinstance(data, pyiqfeed.exceptions.NoDataError):
                        data = None
            else:
                data = method(*f)

            if adjust_data and data is not None:
                col = 'open_p' if 'open_p' in data[0].dtype.names else 'ask' if 'ask' in data[0].dtype.names else None

                if col is not None and 0 in data[col]:
                    logging.getLogger(__name__).warning(f.ticker + " contains 0 in the " + col + " column before adjust")

                adjust(data, Fundamentals.get(f.ticker, self.streaming_conn))

                if col is not None and 0 in data[col]:
                    logging.getLogger(__name__).warning(f.ticker + " contains 0 in the " + col + " column before adjust")
        except pyiqfeed.exceptions.NoDataError as err:
            if self.db is not None:
                with self.db.begin(write=True) as txn:
                    txn.put(bytearray(f.__str__(), encoding='ascii'), pickle.dumps(err))

            return None

        return data

    def _process_data(self, data, data_filter):
        if isinstance(data_filter, TicksFilter) or isinstance(data_filter, TicksForDaysFilter) or isinstance(data_filter, TicksInPeriodFilter):
            return self._process_ticks(data, data_filter)
        elif isinstance(data_filter, BarsFilter) or isinstance(data_filter, BarsForDaysFilter) or isinstance(data_filter, BarsInPeriodFilter):
            return self._process_bars(data, data_filter)
        elif isinstance(data_filter, BarsDailyFilter) or isinstance(data_filter, BarsDailyForDatesFilter) or isinstance(data_filter, BarsWeeklyFilter) or isinstance(data_filter, BarsMonthlyFilter):
            return self._process_daily(data, data_filter)

    def _process_ticks(self, data, data_filter):
        result = pd.DataFrame(data)
        sf = self.key_suffix
        result['Time Stamp' + sf] = data['date'] + data['time']
        result.drop(['date', 'time'], axis=1, inplace=True)
        result.rename_axis({"last": "Last" + sf, "last_sz": "Last Size" + sf, "tot_vlm": "Total Volume" + sf, "bid": "Bid" + sf, "ask": "Ask" + sf, "tick_id": "TickID" + sf, "last_type": "Basis For Last" + sf, "mkt_ctr": "Trade Market Center" + sf}, axis="columns", copy=False, inplace=True)
        result['Symbol'] = data_filter.ticker

        return result

    def _process_bars(self, data, data_filter):
        result = pd.DataFrame(data)
        sf = self.key_suffix
        result['Time Stamp' + sf] = data['date'] + data['time']
        result.drop(['date', 'time'], axis=1, inplace=True)
        result.rename_axis({"high_p": "High" + sf, "low_p": "Low" + sf, "open_p": "Open" + sf, "close_p": "Close" + sf, "tot_vlm": "Total Volume" + sf, "prd_vlm": "Period Volume" + sf, "num_trds": "Number of Trades" + sf}, axis="columns", copy=False, inplace=True)
        result['Symbol'] = data_filter.ticker

        return result

    def _process_daily(self, data, data_filter):
        result = pd.DataFrame(data)
        sf = self.key_suffix
        result.rename_axis({"date": "Date" + sf, "high_p": "High" + sf, "low_p": "Low" + sf, "open_p": "Open" + sf, "close_p": "Close" + sf, "prd_vlm": "Period Volume" + sf, "open_int": "Open Interest" + sf}, axis="columns", copy=False, inplace=True)
        result['Symbol'] = data_filter.ticker

        return result

    @staticmethod
    def _event_type(data_filter):
        if isinstance(data_filter, TicksFilter) or isinstance(data_filter, TicksForDaysFilter) or isinstance(data_filter, TicksInPeriodFilter):
            return 'level_1_tick'
        elif isinstance(data_filter, BarsFilter) or isinstance(data_filter, BarsForDaysFilter) or isinstance(data_filter, BarsInPeriodFilter) or isinstance(data_filter, BarsDailyForDatesFilter):
            return 'bar'
        elif isinstance(data_filter, BarsDailyFilter) or isinstance(data_filter, BarsWeeklyFilter) or isinstance(data_filter, BarsMonthlyFilter):
            return 'daily'

    @events.after
    def no_more_data(self):
        return {'type': 'no_data'}

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


class InPeriodProvider(FilterProvider, metaclass=ABCMeta):
    """
    Generate a sequence of InPeriod filters to obtain market history
    """

    def __init__(self, ticker: typing.Union[list, str], bgn_prd: datetime.date, delta: datetime.timedelta, ascend: bool=True, max_ticks: int=None, timeout: int=None):
        self.ticker = ticker
        self.bgn_prd = datetime.datetime(year=bgn_prd.year, month=bgn_prd.month, day=bgn_prd.day)
        self.delta = delta
        self.ascend = ascend
        self.max_ticks = max_ticks
        self.timeout = timeout

    def __iter__(self):
        self._deltas = -1
        return self

    def __next__(self) -> NamedTuple:
        self._deltas += 1

        now = datetime.datetime.now()

        if self.ascend:
            bgn_prd = self.bgn_prd + self._deltas * self.delta

            if bgn_prd < now:
                end_prd = self.bgn_prd + (self._deltas + 1) * self.delta
                end_prd = end_prd if end_prd < now else now

                return self._create_filter(bgn_prd, end_prd)
            else:
                raise StopIteration
        else:
            if not hasattr(self, 'start'):
                self.start = datetime.datetime(year=now.year, month=now.month, day=now.day + 1)

            end_prd = self.start - self._deltas * self.delta

            if end_prd > self.bgn_prd:
                bgn_prd = max(self.start - (self._deltas + 1) * self.delta, self.bgn_prd)

                return self._create_filter(bgn_prd, end_prd)
            else:
                raise StopIteration

    @abc.abstractmethod
    def _create_filter(self, bgn_prd, end_prd) -> NamedTuple:
        pass


class TicksInPeriodProvider(InPeriodProvider):
    """
    Generate a sequence of TicksInPeriod filters to obtain market history
    """

    def __init__(self, ticker: typing.Union[list, str], bgn_prd: datetime.date, delta: datetime.timedelta, bgn_flt: datetime.time=None, end_flt: datetime.time=None, ascend: bool=False, max_ticks: int=None, timeout: int=None):
        super().__init__(ticker=ticker, bgn_prd=bgn_prd, delta=delta, ascend=ascend, max_ticks=max_ticks, timeout=timeout)
        self.bgn_flt = bgn_flt
        self.end_flt = end_flt

    def __iter__(self):
        self._deltas = -1
        return self

    def _create_filter(self, bgn_prd, end_prd) -> NamedTuple:
        return TicksInPeriodFilter(ticker=self.ticker, bgn_prd=bgn_prd, end_prd=end_prd, bgn_flt=self.bgn_flt, end_flt=self.end_flt, ascend=self.ascend, max_ticks=self.max_ticks, timeout=self.timeout)


class BarsInPeriodProvider(InPeriodProvider):
    """
    Generate a sequence of BarsInPeriod filters to obtain market history
    """

    def __init__(self, ticker: typing.Union[list, str], interval_len: int, interval_type: str, bgn_prd: datetime.date, delta: datetime.timedelta, bgn_flt: datetime.time=None, end_flt: datetime.time=None, ascend: bool=True, max_ticks: int=None, timeout: int=None):
        super().__init__(ticker=ticker, bgn_prd=bgn_prd, delta=delta, ascend=ascend, max_ticks=max_ticks, timeout=timeout)

        self.interval_len = interval_len
        self.interval_type = interval_type
        self.bgn_flt = bgn_flt
        self.end_flt = end_flt

    def __iter__(self):
        self._deltas = -1
        return self

    def _create_filter(self, bgn_prd, end_prd) -> NamedTuple:
        return BarsInPeriodFilter(ticker=self.ticker, interval_len=self.interval_len, interval_type=self.interval_type, bgn_prd=bgn_prd, end_prd=end_prd, bgn_flt=self.bgn_flt, end_flt=self.end_flt, ascend=self.ascend, max_ticks=self.max_ticks, timeout=self.timeout)
