import abc
import datetime
import logging
import queue
import threading
import typing
from multiprocessing.pool import ThreadPool

import pandas as pd

import pyevents.events as events
import pyiqfeed
import pyiqfeed as iq
from atpy.data.iqfeed.filters import *
from atpy.data.iqfeed.iqfeed_level_1_provider import get_fundamentals
from atpy.data.iqfeed.util import launch_service, adjust, IQFeedDataProvider


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


class IQFeedHistoryProvider(object):
    """
    IQFeed historical data provider. See the unit test on how to use
    """

    def __init__(self, num_connections=10, key_suffix=''):
        """
        :param num_connections: number of connections to use when requesting data
        :param key_suffix: suffix for field names
        """
        self.num_connections = num_connections
        self.key_suffix = key_suffix
        self.conn = None
        self.streaming_conn = None
        self.current_batch = None
        self.current_filter = None

    def __enter__(self):
        launch_service()

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

    def request_data(self, f, sync_timestamps=True, adjust_data=True):
        """
        request history data
        :param f: filter tuple
        :param sync_timestamps: synchronize timestamps between symbols
        :param adjust_data: whether to adjust the data
        :return:
        """
        if isinstance(f.ticker, str):
            data = self.request_raw_symbol_data(f, self.conn[0] if isinstance(self.conn, list) else self.conn)
            if data is None:
                logging.getLogger(__name__).warning("No data found for filter: " + str(f))
                return

            data = self._process_data(data, f, adjust_data=adjust_data)

            return data
        elif isinstance(f.ticker, list):
            q = queue.Queue()
            self.request_data_by_filters([f._replace(ticker=t) for t in f.ticker], q, adjust_data=adjust_data)

            signals = {d[0].ticker: d[1] for d in iter(q.get, None)}

            if sync_timestamps:
                signals = self.synchronize_timestamps(signals, f)
            elif len(signals) > 0:
                signals = pd.concat(signals)
                signals.index.set_names('symbol', level=0, inplace=True)
                signals.sort_index(inplace=True, ascending=f.ascend)

            return signals if len(signals) > 0 else None

    def request_data_by_filters(self, filters: list, q: queue.Queue, adjust_data=True):
        """
        request data for multiple filters
        :param filters: list of filters
        :param q: queue to populate the results as they come. When all the results are returned, None is inserted to signal that no more are coming.
        :param adjust_data: adjust the data or not
        :return: None
        """
        if self.num_connections > 1:
            pool = ThreadPool(self.num_connections)
            self._global_counter = 0
            lock = threading.Lock()
            no_data = set()

            def mp_worker(p):
                ft, conn = p

                try:
                    raw_data = self.request_raw_symbol_data(ft, conn)
                except Exception as err:
                    raw_data = None
                    logging.getLogger(__name__).exception(err)

                if raw_data is not None:
                    q.put((ft, self._process_data(raw_data, ft, adjust_data=adjust_data)))
                else:
                    no_data.add(ft)

                with lock:
                    self._global_counter += 1
                    if self._global_counter == len(filters):
                        q.put(None)

                    if self._global_counter % 20 == 0 or self._global_counter == len(filters):
                        logging.getLogger(__name__).info("Loaded " + str(self._global_counter) + " symbols")
                        if len(no_data) > 0:
                            no_data_list = list(no_data)
                            no_data_list.sort()
                            logging.getLogger(__name__).info("No data found for " + str(len(no_data_list)) + " symbols: " + str(no_data_list))
                            no_data.clear()

            pool.map(mp_worker, ((f, self.conn[i % self.num_connections]) for i, f in enumerate(filters)))
            pool.close()
            del self._global_counter
        else:
            for ft in filters:
                data = self.request_raw_symbol_data(ft, self.conn)
                if data is not None:
                    q.put((ft, self._process_data(data, ft, adjust_data=adjust_data)))

            q.put(None)

    def synchronize_timestamps(self, signals: map, f: NamedTuple):
        """
        synchronize timestamps between historical signals
        :param signals: map of dataframes for each equity
        :param f: filter tuple
        :return:
        """
        if signals is None or len(signals) <= 1:
            result = signals
        elif 'tick_id' + self.key_suffix in iter(signals.values()).__next__():
            signals = pd.concat(signals)
            signals.index.set_names('symbol', level=0, inplace=True)
            signals.sort_index(level=['symbol', 'tick_id'], inplace=True, ascending=f.ascend)

            result = signals
        else:
            col = 'timestamp' + self.key_suffix if 'timestamp' + self.key_suffix in list(signals.values())[0] else 'date' + self.key_suffix if 'date' + self.key_suffix in list(signals.values())[0] else None
            if col is not None:
                signals = pd.concat(signals)
                signals.index.set_names('symbol', level=0, inplace=True)

                for symbol in signals.index.get_level_values('symbol').unique():
                    if 0 in signals.loc[symbol, 'open'].values:
                        logging.getLogger(__name__).warning(symbol + " contains 0 in the Open column before timestamp sync")

                multi_index = pd.MultiIndex.from_product([signals['symbol'].unique(), signals[col].unique()], names=['symbol', col]).sort_values()

                signals = signals.reindex(multi_index)
                signals.drop(['symbol', col], axis=1, inplace=True)
                signals.reset_index(inplace=True)
                signals.set_index(multi_index, inplace=True)

                for c in [c for c in ['period_volume', 'number_of_trades'] if c in signals.columns]:
                    signals[c].fillna(0, inplace=True)

                if 'close' in signals.columns:
                    signals['close'] = signals.groupby(level=0)['close'].fillna(method='ffill')

                    if self.current_filter is not None and type(self.current_filter) == type(f) and self.current_batch is not None and f.ascend is True and self.current_batch.index.levels[0].equals(signals.index.levels[0]):
                        last = self.current_batch.groupby(level=0)['close'].last()
                        signals['close'] = signals.groupby(level=0)['close'].apply(lambda x: x.fillna(last[last.index.get_loc(x.name)]))

                    signals['close'] = signals.groupby(level=0)['close'].fillna(method='backfill')

                    op = signals['close']

                    for c in [c for c in ['open', 'high', 'low'] if c in signals.columns]:
                        signals[c].fillna(op, inplace=True)

                signals = signals.groupby(level=0).fillna(method='ffill')

                if self.current_filter is not None and type(self.current_filter) == type(f) and self.current_batch is not None and f.ascend is True and self.current_batch.index.levels[0].equals(signals.index.levels[0]):
                    last = self.current_batch.groupby(level=0).last()
                    signals = signals.groupby(level=0).apply(lambda x: x.fillna(last.iloc[last.index.get_loc(x.name)]))

                signals = signals.groupby(level=0).fillna(method='backfill')

                zero_values = list()

                for symbol in signals.index.get_level_values('symbol').unique():
                    if 0 in signals.loc[symbol, 'open'].values:
                        logging.getLogger(__name__).warning(symbol + " contains 0 in the Open column after timestamp sync")
                        zero_values.append(symbol)

                if not f.ascend:
                    signals.sort_index(level=['symbol', col], inplace=True, ascending=False)

                result = signals

            logging.getLogger(__name__).info("Generated data of shape: " + str(result.shape))

        return result

    @staticmethod
    def request_raw_symbol_data(f, conn):
        if isinstance(f, TicksFilter):
            method = conn.request_ticks
        elif isinstance(f, TicksForDaysFilter):
            method = conn.request_ticks_for_days
        elif isinstance(f, TicksInPeriodFilter):
            method = conn.request_ticks_in_period
        elif isinstance(f, BarsFilter):
            method = conn.request_bars
        elif isinstance(f, BarsForDaysFilter):
            method = conn.request_bars_for_days
        elif isinstance(f, BarsInPeriodFilter):
            method = conn.request_bars_in_period
        elif isinstance(f, BarsDailyFilter):
            method = conn.request_daily_data
        elif isinstance(f, BarsDailyForDatesFilter):
            method = conn.request_daily_data_for_dates
        elif isinstance(f, BarsWeeklyFilter):
            method = conn.request_weekly_data
        elif isinstance(f, BarsMonthlyFilter):
            method = conn.request_monthly_data

        try:
            data = method(*f)

            if data is not None:
                col = 'open_p' if 'open_p' in data[0].dtype.names else 'ask' if 'ask' in data[0].dtype.names else None

                if col is not None and 0 in data[col]:
                    logging.getLogger(__name__).warning(f.ticker + " contains 0 in the " + col)
        except pyiqfeed.exceptions.NoDataError:
            return None

        return data

    def _process_data(self, data, data_filter, adjust_data=True):
        if isinstance(data_filter, TicksFilter) or isinstance(data_filter, TicksForDaysFilter) or isinstance(data_filter, TicksInPeriodFilter):
            return self._process_ticks(data, data_filter, adjust_data)
        elif isinstance(data_filter, BarsFilter) or isinstance(data_filter, BarsForDaysFilter) or isinstance(data_filter, BarsInPeriodFilter):
            return self._process_bars(data, data_filter, adjust_data)
        elif isinstance(data_filter, BarsDailyFilter) or isinstance(data_filter, BarsDailyForDatesFilter) or isinstance(data_filter, BarsWeeklyFilter) or isinstance(data_filter, BarsMonthlyFilter):
            return self._process_daily(data, data_filter)

    def _process_ticks(self, data, data_filter, adjust_data=True):
        result = pd.DataFrame(data)
        sf = self.key_suffix

        result['timestamp' + sf] = pd.Index(data['date'] + data['time']).tz_localize('US/Eastern').tz_convert('UTC')
        result.set_index('timestamp' + sf, inplace=True, drop=False)
        result.drop(['date', 'time'], axis=1, inplace=True)

        result.rename({"last": "last" + sf, "last_sz": "last_size" + sf, "tot_vlm": "total_volume" + sf, "bid": "bid" + sf, "ask": "ask" + sf, "tick_id": "tick_id" + sf, "last_type": "basis_for_last" + sf, "mkt_ctr": "trade_market_center" + sf}, axis="columns", copy=False, inplace=True)
        result['symbol'] = data_filter.ticker

        result.set_index("tick_id" + sf, inplace=True, drop=False)

        if adjust_data:
            adjust(result, get_fundamentals(data_filter.ticker, self.streaming_conn))

        return result

    def _process_bars(self, data, data_filter, adjust_data=True):
        result = pd.DataFrame(data)
        sf = self.key_suffix

        result['timestamp' + sf] = pd.Index(data['date'] + data['time']).tz_localize('US/Eastern').tz_convert('UTC')
        result.set_index('timestamp' + sf, inplace=True, drop=False)
        result.drop(['date', 'time'], axis=1, inplace=True)

        result.rename({"high_p": "high" + sf, "low_p": "low" + sf, "open_p": "open" + sf, "close_p": "close" + sf, "tot_vlm": "total_volume" + sf, "prd_vlm": "period_volume" + sf, "num_trds": "number_of_trades" + sf}, axis="columns", copy=False, inplace=True)
        result['symbol'] = data_filter.ticker

        if adjust_data:
            adjust(result, get_fundamentals(data_filter.ticker, self.streaming_conn))

        return result

    def _process_daily(self, data, data_filter):
        result = pd.DataFrame(data)
        sf = self.key_suffix
        result.rename({"date": "date" + sf, "high_p": "high" + sf, "low_p": "low" + sf, "open_p": "open" + sf, "close_p": "close" + sf, "prd_vlm": "period_volume" + sf, "open_int": "open_interest" + sf}, axis="columns", copy=False, inplace=True)
        result['symbol'] = data_filter.ticker

        result.set_index('date' + sf, inplace=True, drop=False)

        return result

    @staticmethod
    def _event_type(data_filter):
        if isinstance(data_filter, TicksFilter) or isinstance(data_filter, TicksForDaysFilter) or isinstance(data_filter, TicksInPeriodFilter):
            return 'level_1_tick'
        elif isinstance(data_filter, BarsFilter) or isinstance(data_filter, BarsForDaysFilter) or isinstance(data_filter, BarsInPeriodFilter) or isinstance(data_filter, BarsDailyForDatesFilter):
            return 'bar'
        elif isinstance(data_filter, BarsDailyFilter) or isinstance(data_filter, BarsWeeklyFilter) or isinstance(data_filter, BarsMonthlyFilter):
            return 'daily'


class IQFeedHistoryListener(IQFeedHistoryProvider, metaclass=events.GlobalRegister):
    """
    IQFeed historical data listener. See the unit test on how to use
    """

    def __init__(self, minibatch=None, fire_batches=False, fire_ticks=False, run_async=True, adjust_data=True, num_connections=10, key_suffix='', filter_provider=None, sync_timestamps=True):
        """
        :param minibatch: size of the minibatch
        :param fire_batches: raise event for each batch
        :param fire_ticks: raise event for each tick
        :param run_async: run asynchronous
        :param adjust_data: adjust data
        :param num_connections: number of connections to use when requesting data
        :param key_suffix: suffix for field names
        :param filter_provider: news filter list
        :param sync_timestamps: synchronize timestamps for each symbol
        """
        super().__init__(num_connections=num_connections, key_suffix=key_suffix)

        self.minibatch = minibatch
        self.fire_batches = fire_batches
        self.fire_ticks = fire_ticks
        self.adjust_data = adjust_data
        self.run_async = run_async
        self.current_minibatch = None
        self.filter_provider = filter_provider
        self._is_running = False
        self._background_thread = None
        self.conn = None
        self.streaming_conn = None
        self.sync_timestamps = sync_timestamps

    def __exit__(self, exception_type, exception_value, traceback):
        if self._background_thread is not None and self._background_thread.is_alive():
            self._is_running = False
            self._background_thread.join()
        else:
            self._is_running = False

        self._background_thread = None

        self.no_more_data()

        super().__exit__(exception_type, exception_value, traceback)

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

            d = self.request_data(f, sync_timestamps=self.sync_timestamps, adjust_data=self.adjust_data)

            self.current_filter = f
            self.current_batch = d

            yield d, f

        self.no_more_data()

    def stop(self):
        self._is_running = False

    def fire_events(self, data, f):
        event_type = self._event_type(f)
        if data is None:
            return

        if isinstance(data.index, pd.DatetimeIndex):
            if self.fire_ticks:
                for i in range(data.shape[0]):
                    self.process_datum({'type': event_type, 'data': data.iloc[i]})

            if self.minibatch is not None:
                if self.current_minibatch is None or (self.current_filter is not None and type(self.current_filter) != type(f)):
                    self.current_minibatch = data.copy(deep=True)
                else:
                    self.current_minibatch = pd.concat([self.current_minibatch, data], axis=0)
                    self.current_minibatch.sort_index(inplace=True, ascending=f.ascend)

                for i in range(self.minibatch, self.current_minibatch.shape[0] - self.current_minibatch.shape[0] % self.minibatch + 1, self.minibatch):
                    mb = self.current_minibatch.iloc[i - self.minibatch: i]
                    mb.set_index(keys='tick_id' if 'tick_id' in mb.columns else 'timestamp' if 'timestamp' in mb.columns else 'date', drop=False, inplace=True)
                    self.process_minibatch({'type': event_type + '_mb', 'data': mb})

                self.current_minibatch = None if self.current_minibatch.shape[0] - i == 0 else self.current_minibatch.iloc[i:]
                if self.current_minibatch is not None:
                    self.current_minibatch.set_index(keys='tick_id' if 'tick_id' in self.current_minibatch.columns else 'timestamp' if 'timestamp' in self.current_minibatch.columns else 'date', drop=False, inplace=True)

            if self.fire_batches:
                self.process_batch({'type': event_type + '_batch', 'data': data})

        elif 'timestamp' in data.index.names:
            if self.fire_ticks:
                for i in range(data.index.levels[1].shape[0]):
                    self.process_datum({'type': event_type, 'data': data.groupby(level=0).nth(i)})

            if self.minibatch is not None:
                if self.current_minibatch is None or (self.current_filter is not None and type(self.current_filter) != type(f)):
                    self.current_minibatch = data.copy(deep=True)
                else:
                    self.current_minibatch = pd.concat([self.current_minibatch, data], axis=0)
                    self.current_minibatch.sort_index(inplace=True, ascending=f.ascend)

                for i in range(self.minibatch, self.current_minibatch.index.levels[1].shape[0] - self.current_minibatch.index.levels[1].shape[0] % self.minibatch + 1, self.minibatch):
                    self.process_minibatch({'type': event_type + '_mb', 'data': self.current_minibatch.loc[pd.IndexSlice[:, data.index.levels[1][i - self.minibatch: i]], :]})

                self.current_minibatch = None if self.current_minibatch.index.levels[1].shape[0] - i == 0 else self.current_minibatch.groupby(level=0).tail(self.current_minibatch.index.levels[1].shape[0] - i)

            if self.fire_batches:
                self.process_batch({'type': event_type + '_batch', 'data': data})
        elif 'tick_id' in data.index.names:
            if self.fire_ticks:
                for i in range(data.shape[0]):
                    self.process_datum({'type': event_type, 'data': data.iloc[i]})

            if self.minibatch is not None:
                if self.current_minibatch is None or (self.current_filter is not None and type(self.current_filter) != type(f)):
                    self.current_minibatch = data.copy(deep=True)
                else:
                    self.current_minibatch = pd.concat([self.current_minibatch, data], axis=0)
                    self.current_minibatch.sort_index(inplace=True, ascending=f.ascend)

                for i in range(self.minibatch, self.current_minibatch.shape[0] - self.current_minibatch.shape[0] % self.minibatch + 1, self.minibatch):
                    mb = self.current_minibatch.iloc[i - self.minibatch: i]
                    mb.set_index(keys=['symbol', 'tick_id'], drop=False, inplace=True)
                    self.process_minibatch({'type': event_type + '_mb', 'data': mb})

                self.current_minibatch = None if self.current_minibatch.shape[0] - i == 0 else self.current_minibatch.iloc[i:]
                if self.current_minibatch is not None:
                    self.current_minibatch.set_index(keys=['symbol', 'tick_id'], drop=False, inplace=True)

            if self.fire_batches:
                self.process_batch({'type': event_type + '_batch', 'data': data})

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

    def __init__(self, ticker: typing.Union[list, str], bgn_prd: datetime.date, delta: datetime.timedelta, ascend: bool=True, max_ticks: int=None, timeout: int=None, overlap: datetime.timedelta=None):
        """
        :param overlap: whether to provide overlap within the intervals
        """

        self.ticker = ticker
        self.bgn_prd = datetime.datetime(year=bgn_prd.year, month=bgn_prd.month, day=bgn_prd.day)
        self.delta = delta
        self.ascend = ascend
        self.max_ticks = max_ticks
        self.timeout = timeout
        self.overlap = overlap if overlap is not None else datetime.timedelta(days=0)

    def __iter__(self):
        self._deltas = -1
        return self

    def __next__(self) -> NamedTuple:
        self._deltas += 1

        now = datetime.datetime.now()

        if self.ascend:
            bgn_prd = self.bgn_prd + self._deltas * self.delta

            if bgn_prd < now:
                end_prd = self.bgn_prd + (self._deltas + 1) * self.delta + self.overlap
                end_prd = end_prd if end_prd < now else now

                return self._create_filter(bgn_prd, end_prd)
            else:
                raise StopIteration
        else:
            if not hasattr(self, 'start'):
                self.start = datetime.datetime(year=now.year, month=now.month, day=now.day + 1)

            end_prd = self.start - self._deltas * self.delta

            if end_prd > self.bgn_prd:
                bgn_prd = max(self.start - (self._deltas + 1) * self.delta - self.overlap, self.bgn_prd)

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

    def __init__(self, ticker: typing.Union[list, str], interval_len: int, interval_type: str, bgn_prd: datetime.date, delta: datetime.timedelta, bgn_flt: datetime.time=None, end_flt: datetime.time=None, ascend: bool=True, max_ticks: int=None, timeout: int=None, overlap: datetime.timedelta=None):
        super().__init__(ticker=ticker, bgn_prd=bgn_prd, delta=delta, ascend=ascend, max_ticks=max_ticks, timeout=timeout, overlap=overlap)

        self.interval_len = interval_len
        self.interval_type = interval_type
        self.bgn_flt = bgn_flt
        self.end_flt = end_flt

    def __iter__(self):
        self._deltas = -1
        return self

    def _create_filter(self, bgn_prd, end_prd) -> NamedTuple:
        return BarsInPeriodFilter(ticker=self.ticker, interval_len=self.interval_len, interval_type=self.interval_type, bgn_prd=bgn_prd, end_prd=end_prd, bgn_flt=self.bgn_flt, end_flt=self.end_flt, ascend=self.ascend, max_ticks=self.max_ticks, timeout=self.timeout)
