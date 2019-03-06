import datetime
import functools
import logging
import os
import typing

import psycopg2
from dateutil.relativedelta import relativedelta

import atpy.data.iqfeed.util as iqutil
from atpy.backtesting.data_replay import DataReplayEvents, DataReplay
from atpy.backtesting.mock_exchange import MockExchange, StaticSlippageLoss, PerShareCommissionLoss
from atpy.backtesting.random_strategy import RandomStrategy
from atpy.data.cache.lmdb_cache import read_pickle
from atpy.data.cache.postgres_cache import BarsInPeriodProvider
from atpy.data.quandl.postgres_cache import SFInPeriodProvider
from atpy.data.ts_util import current_period, current_phase, gaps, rolling_mean, AsyncInPeriodProvider
from atpy.portfolio.portfolio_manager import PortfolioManager


def data_replay_events(listeners):
    return DataReplayEvents(listeners, DataReplay(), event_name='data')


def add_postgres_ohlc_1m(dre: DataReplayEvents, bgn_prd: datetime.datetime, historical_depth=300, run_async=False, url: str = None, lmdb_path: str = None):
    """
    Create DataReplay environment for bar data using PostgreSQL
    :param dre: DataReplayEvents
    :param bgn_prd: begin period
    :param historical_depth: historical depth for source
    :param run_async: generate data asynchronously
    :param url: postgre url (can be obtained via env variable)
    :param lmdb_path: path to lmdb cache file
    :return: filter function, that only accepts this event
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    lmdb_path = os.environ['ATPY_LMDB_PATH'] if lmdb_path is None and 'ATPY_LMDB_PATH' in os.environ else lmdb_path
    cache = functools.partial(read_pickle, lmdb_path=lmdb_path) if lmdb_path is not None else None

    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=60, interval_type='s', bars_table='bars_1m', bgn_prd=bgn_prd, delta=relativedelta(weeks=1), overlap=relativedelta(microseconds=-1), cache=cache)
    if run_async:
        bars_in_period = AsyncInPeriodProvider(bars_in_period)

    dre.data_replay.add_source(bars_in_period, 'bars_1m', historical_depth=historical_depth, listeners=dre.listeners)

    return dre.event_filter_by_source('bars_1m'), dre.event_filter_function('bars_1m')


def add_postgres_ohlc_5m(dre: DataReplayEvents, bgn_prd: datetime.datetime, historical_depth=300, run_async=False, url: str = None, lmdb_path: str = None):
    """
    Create DataReplay environment for bar data using PostgreSQL
    :param dre: DataReplayEvents
    :param bgn_prd: begin period
    :param historical_depth: historical depth for source
    :param run_async: generate data asynchronously
    :param url: postgre url (can be obtained via env variable)
    :param lmdb_path: path to lmdb cache file
    :return: filter function, that only accepts this event
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    lmdb_path = os.environ['ATPY_LMDB_PATH'] if lmdb_path is None and 'ATPY_LMDB_PATH' in os.environ else lmdb_path
    cache = functools.partial(read_pickle, lmdb_path=lmdb_path) if lmdb_path is not None else None

    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=300, interval_type='s', bars_table='bars_5m', bgn_prd=bgn_prd, delta=relativedelta(weeks=1), overlap=relativedelta(microseconds=-1), cache=cache)
    if run_async:
        bars_in_period = AsyncInPeriodProvider(bars_in_period)

    dre.data_replay.add_source(bars_in_period, 'bars_5m', historical_depth=historical_depth, listeners=dre.listeners)

    return dre.event_filter_by_source('bars_5m'), dre.event_filter_function('bars_5m')


def add_postgres_ohlc_60m(dre: DataReplayEvents, bgn_prd: datetime.datetime, historical_depth=24, run_async=False, url: str = None, lmdb_path: str = None):
    """
    Create DataReplay environment for bar data using PostgreSQL
    :param dre: DataReplayEvents
    :param bgn_prd: begin period
    :param historical_depth: historical depth for source
    :param run_async: generate data asynchronously
    :param url: postgre url (can be obtained via env variable)
    :param lmdb_path: path to lmdb cache file
    :return: filter function, that only accepts this event
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    lmdb_path = os.environ['ATPY_LMDB_PATH'] if lmdb_path is None and 'ATPY_LMDB_PATH' in os.environ else lmdb_path
    cache = functools.partial(read_pickle, lmdb_path=lmdb_path) if lmdb_path is not None else None

    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=3300, interval_type='s', bars_table='bars_60m', bgn_prd=bgn_prd, delta=relativedelta(weeks=1), overlap=relativedelta(microseconds=-1), cache=cache)
    if run_async:
        bars_in_period = AsyncInPeriodProvider(bars_in_period)

    dre.data_replay.add_source(bars_in_period, 'bars_60m', historical_depth=historical_depth, listeners=dre.listeners)

    return dre.event_filter_by_source('bars_60m'), dre.event_filter_function('bars_60m')


def add_postgres_ohlc_1d(dre: DataReplayEvents, bgn_prd: datetime.datetime, historical_depth=50, run_async=False, url: str = None, lmdb_path: str = None):
    """
    Create DataReplay environment for bar data using PostgreSQL
    :param dre: DataReplayEvents
    :param bgn_prd: begin period
    :param historical_depth: historical depth for source
    :param run_async: generate data asynchronously
    :param url: postgre url (can be obtained via env variable)
    :param lmdb_path: path to lmdb cache file
    :return: filter function, that only accepts this event
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    lmdb_path = os.environ['ATPY_LMDB_PATH'] if lmdb_path is None and 'ATPY_LMDB_PATH' in os.environ else lmdb_path
    cache = functools.partial(read_pickle, lmdb_path=lmdb_path) if lmdb_path is not None else None

    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=1, interval_type='d', bars_table='bars_1d', bgn_prd=bgn_prd, delta=relativedelta(weeks=1), overlap=relativedelta(microseconds=-1), cache=cache)
    if run_async:
        bars_in_period = AsyncInPeriodProvider(bars_in_period)

    dre.data_replay.add_source(bars_in_period, 'bars_1d', historical_depth=historical_depth, listeners=dre.listeners)

    return dre.event_filter_by_source('bars_1d'), dre.event_filter_function('bars_1d')


_symbols = None


def add_iq_symbol_data(listeners, symbols_file: str = None):
    """
    Append symbol data from IQFeed to each event
    """

    global _symbols
    _symbols = iqutil.get_symbols(symbols_file=symbols_file)

    def iq_symbol_data(e):
        if e['type'] == 'data':
            e['iq_symbol_data'] = _symbols

    listeners += iq_symbol_data


def add_quandl_sf(dre: DataReplayEvents, bgn_prd: datetime.datetime, dataset_name: str = 'SF0', url: str = None):
    """
    Append Quandl SF0 (or SF1) database to the event. Data will be streamed
    :return: filter function, that only accepts this event
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    name = 'quandl_' + dataset_name.lower()
    sf_in_period = SFInPeriodProvider(conn=con, bgn_prd=bgn_prd, delta=relativedelta(years=1), overlap=relativedelta(microseconds=-1), table_name=name)
    dre.data_replay.add_source(sf_in_period, name=name, historical_depth=200)

    return dre.event_filter_by_source(name)


def add_rolling_mean(filtered_event_stream, window: int, column: typing.Union[typing.List, str] = 'close'):
    """
    Compute the rolling mean over a column
    :param filtered_event_stream: the filtered event stream, consisting only of the required data points
    :param window: window size OHLC DataFrame
    :param column: a column (or list of columns, where to apply the rolling mean)
    :return DataFrame with changes
    """

    def rm(datum):
        datum[column + '_rm_' + str(window)] = rolling_mean(df=datum, window=window, column=column)

    filtered_event_stream += rm


def add_current_period(listeners, event_filter_function):
    """
    Append only current period (trading/after-hours) for the event data
    :param listeners: data replay event stream (full events)
    :param event_filter_function: unfiltered event stream
    """

    prev_period = None

    def current_p(e):
        datum = event_filter_function(e)
        if datum is not None:
            nonlocal prev_period

            for k in e:
                if e[k] is datum:
                    e[k + '_current_period'], e['period_name'] = current_period(datum)

                    e['period_start'] = True if prev_period is not None and prev_period != e['period_name'] else False
                    prev_period = e['period_name']

                    break

    listeners += current_p


def add_daily_log(filtered_event_stream):
    """
    Add log message on each new day
    :param filtered_event_stream: filtered event stream (full events)
    """

    prev_timestamp = None
    current_time = datetime.datetime.now()

    def daily_log(e):
        nonlocal prev_timestamp
        nonlocal current_time

        current_ts = e['timestamp']
        if prev_timestamp is not None and current_ts.day != prev_timestamp.day:
            now = datetime.datetime.now()
            logging.getLogger(__name__).info("Timestamp " + str(current_ts) + "; Time elapsed for the period: " + str(now - current_time))
            current_time = now

        prev_timestamp = current_ts

    filtered_event_stream += daily_log


def add_timestamp_log(filtered_event_stream):
    """
    Add log message on new timestamp
    :param filtered_event_stream: filtered event stream (full events)
    """

    def timestamp_log(e):
        logging.getLogger(__name__).info("Current timestamp: " + str(e['timestamp']))

    filtered_event_stream += timestamp_log


def add_current_phase(filtered_event_stream):
    """
    Append current phase
    :param filtered_event_stream: filtered event stream (full events)
    """

    prev_period = None

    def current_p(e):
        nonlocal prev_period
        e['current_phase'] = current_phase(e['timestamp'])
        e['phase_start'] = True if prev_period is not None and prev_period != e['current_phase'] else False
        prev_period = e['current_phase']

    filtered_event_stream += current_p


def add_gaps(listeners, event_filter_function):
    """
    Append gaps computation for datum_name dataset for every time moment
    :param listeners: data replay event stream (full events)
    :param event_filter_function: unfiltered event stream
    """

    def current_p(e):
        datum = event_filter_function(e)
        if datum is not None:
            for k in e:
                if e[k] is datum:
                    e[k + '_gaps'] = gaps(e[k])
                    break

    listeners += current_p


def add_mock_exchange(listeners, order_requests_stream, bar_event_stream, slippage_loss_ratio: float = None, commission_per_share: float = None):
    """
    Append mock exchange
    :param listeners: listeners environment
    :param order_requests_stream: event stream for order requests
    :param bar_event_stream: event stream for bar data
    :param slippage_loss_ratio: ratio for slippage loss
    :param commission_per_share: broker tax per share
    """

    return MockExchange(listeners=listeners,
                        order_requests_event_stream=order_requests_stream,
                        bar_event_stream=bar_event_stream,
                        order_processor=StaticSlippageLoss(slippage_loss_ratio) if slippage_loss_ratio is not None else None,
                        commission_loss=PerShareCommissionLoss(commission_per_share) if commission_per_share is not None else None)


def add_portfolio_manager(listeners, fulfilled_orders_stream, bar_event_stream, initial_capital: float, existing_orders=None):
    """
    Append portfolio manager
    :param listeners: listeners environment
    :param fulfilled_orders_stream: event stream for fulfilled orders
    :param bar_event_stream: event stream for bar data
    :param initial_capital: starting capital
    :param existing_orders: broker tax per share
    """

    return PortfolioManager(listeners=listeners,
                            initial_capital=initial_capital,
                            fulfilled_orders_event_stream=fulfilled_orders_stream,
                            bar_event_stream=bar_event_stream,
                            orders=existing_orders)


def add_random_strategy(listeners, portfolio_manager: PortfolioManager, bar_event_stream, max_buys_per_step=1, max_sells_per_step=1):
    """
    Append random strategy to the process
    :param listeners: listeners environment
    :param portfolio_manager: Portfolio manager
    :param bar_event_stream: event stream for bar data
    :param max_buys_per_step: maximum buy orders per time step (one bar)
    :param max_sells_per_step: maximum sell orders per time step (one bar)
    """

    return RandomStrategy(listeners=listeners,
                          portfolio_manager=portfolio_manager,
                          bar_event_stream=bar_event_stream,
                          max_buys_per_step=max_buys_per_step,
                          max_sells_per_step=max_sells_per_step)
