import datetime
import functools
import logging
import os
import typing

import psycopg2
from dateutil.relativedelta import relativedelta

import atpy.data.iqfeed.util as iqutil
from atpy.backtesting.data_replay import DataReplayEvents, DataReplay
from atpy.data.cache.lmdb_cache import read_pickle
from atpy.data.cache.postgres_cache import BarsInPeriodProvider
from atpy.data.quandl.postgres_cache import SFInPeriodProvider
from atpy.data.ts_util import current_period, current_phase, gaps, rolling_mean, AsyncInPeriodProvider


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
    :return: dataframe
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    lmdb_path = os.environ['ATPY_LMDB_PATH'] if lmdb_path is None and 'ATPY_LMDB_PATH' in os.environ else lmdb_path
    cache = functools.partial(read_pickle, lmdb_path=lmdb_path) if lmdb_path is not None else None

    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=60, interval_type='s', bars_table='bars_1m', bgn_prd=bgn_prd, delta=relativedelta(weeks=1), overlap=relativedelta(microseconds=-1), cache=cache)
    if run_async:
        bars_in_period = AsyncInPeriodProvider(bars_in_period)

    dre.data_replay.add_source(bars_in_period, 'bars_1m', historical_depth=historical_depth, listeners=dre.listeners)

    return dre


def add_postgres_ohlc_5m(dre: DataReplayEvents, bgn_prd: datetime.datetime, historical_depth=300, run_async=False, url: str = None, lmdb_path: str = None):
    """
    Create DataReplay environment for bar data using PostgreSQL
    :param dre: DataReplayEvents
    :param bgn_prd: begin period
    :param historical_depth: historical depth for source
    :param run_async: generate data asynchronously
    :param url: postgre url (can be obtained via env variable)
    :param lmdb_path: path to lmdb cache file
    :return: dataframe
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    lmdb_path = os.environ['ATPY_LMDB_PATH'] if lmdb_path is None and 'ATPY_LMDB_PATH' in os.environ else lmdb_path
    cache = functools.partial(read_pickle, lmdb_path=lmdb_path) if lmdb_path is not None else None

    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=300, interval_type='s', bars_table='bars_5m', bgn_prd=bgn_prd, delta=relativedelta(weeks=1), overlap=relativedelta(microseconds=-1), cache=cache)
    if run_async:
        bars_in_period = AsyncInPeriodProvider(bars_in_period)

    dre.data_replay.add_source(bars_in_period, 'bars_5m', historical_depth=historical_depth, listeners=dre.listeners)

    return dre


def add_postgres_ohlc_60m(dre: DataReplayEvents, bgn_prd: datetime.datetime, historical_depth=24, run_async=False, url: str = None, lmdb_path: str = None):
    """
    Create DataReplay environment for bar data using PostgreSQL
    :param dre: DataReplayEvents
    :param bgn_prd: begin period
    :param historical_depth: historical depth for source
    :param run_async: generate data asynchronously
    :param url: postgre url (can be obtained via env variable)
    :param lmdb_path: path to lmdb cache file
    :return: dataframe
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    lmdb_path = os.environ['ATPY_LMDB_PATH'] if lmdb_path is None and 'ATPY_LMDB_PATH' in os.environ else lmdb_path
    cache = functools.partial(read_pickle, lmdb_path=lmdb_path) if lmdb_path is not None else None

    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=3300, interval_type='s', bars_table='bars_60m', bgn_prd=bgn_prd, delta=relativedelta(weeks=1), overlap=relativedelta(microseconds=-1), cache=cache)
    if run_async:
        bars_in_period = AsyncInPeriodProvider(bars_in_period)

    dre.data_replay.add_source(bars_in_period, 'bars_60m', historical_depth=historical_depth, listeners=dre.listeners)

    return dre


def add_postgres_ohlc_1d(dre: DataReplayEvents, bgn_prd: datetime.datetime, historical_depth=50, run_async=False, url: str = None, lmdb_path: str = None):
    """
    Create DataReplay environment for bar data using PostgreSQL
    :param dre: DataReplayEvents
    :param bgn_prd: begin period
    :param historical_depth: historical depth for source
    :param run_async: generate data asynchronously
    :param url: postgre url (can be obtained via env variable)
    :param lmdb_path: path to lmdb cache file
    :return: dataframe
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    lmdb_path = os.environ['ATPY_LMDB_PATH'] if lmdb_path is None and 'ATPY_LMDB_PATH' in os.environ else lmdb_path
    cache = functools.partial(read_pickle, lmdb_path=lmdb_path) if lmdb_path is not None else None

    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=1, interval_type='d', bars_table='bars_1d', bgn_prd=bgn_prd, delta=relativedelta(weeks=1), overlap=relativedelta(microseconds=-1), cache=cache)
    if run_async:
        bars_in_period = AsyncInPeriodProvider(bars_in_period)

    dre.data_replay.add_source(bars_in_period, 'bars_1d', historical_depth=historical_depth, listeners=dre.listeners)

    return dre


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
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    name = 'quandl_' + dataset_name.lower()
    sf_in_period = SFInPeriodProvider(conn=con, bgn_prd=bgn_prd, delta=relativedelta(years=1), overlap=relativedelta(microseconds=-1), table_name=name)
    dre.data_replay.add_source(sf_in_period, name=name, historical_depth=200)


def add_rolling_mean(listeners, datum_name: str, window: int, column: typing.Union[typing.List, str] = 'close'):
    """
    Compute the rolling mean over a column
    :param listeners: listeners
    :param datum_name: data name
    :param window: window size OHLC DataFrame
    :param column: a column (or list of columns, where to apply the rolling mean)
    :return DataFrame with changes
    """

    def rm(e):
        if datum_name in e:
            e[datum_name][column + '_rm_' + str(window)] = rolling_mean(df=e[datum_name], window=window, column=column)

    listeners += rm


def add_current_period(listeners, datum_name: str):
    """
    Append only current period (trading/after-hours) for the event data
    :param listeners: listeners environment
    :param datum_name: the name of the DataFrame in the event. Could be bars_1m, bars_5m, etc.
    """

    prev_period = None

    def current_p(e):
        if 'timestamp' in e:
            nonlocal prev_period

            if datum_name in e:
                e[datum_name + '_current_phase'], e['current_phase'] = current_period(e[datum_name])
            else:
                e['current_phase'] = current_phase(e['timestamp'])

            e['phase_start'] = True if prev_period is not None and prev_period != e['current_phase'] else False
            prev_period = e['current_phase']

    listeners += current_p


def add_daily_log(listeners):
    """
    Add log message on each new day
    :param listeners: listeners environment
    """

    prev_timestamp = None
    current_time = datetime.datetime.now()

    def daily_log(e):
        if 'timestamp' in e:
            nonlocal prev_timestamp
            nonlocal current_time

            current_ts = e['timestamp']
            if prev_timestamp is not None and current_ts.day != prev_timestamp.day:
                now = datetime.datetime.now()
                logging.getLogger(__name__).info("Timestamp " + str(current_ts) + "; Time elapsed for the period: " + str(now - current_time))
                current_time = now

            prev_timestamp = current_ts

    listeners += daily_log


def add_timestamp_log(listeners):
    """
    Add log message on new timestamp
    :param listeners: listeners environment
    """

    def timestamp_log(e):
        if 'timestamp' in e:
            logging.getLogger(__name__).info("Current timestamp: " + str(e['timestamp']))

    listeners += timestamp_log


def add_current_phase(listeners):
    """
    Append current phase
    :param listeners: listeners environment
    """

    prev_period = None

    def current_p(e):
        if 'timestamp' in e:
            nonlocal prev_period
            e['current_phase'] = current_phase(e['timestamp'])
            e['phase_start'] = True if prev_period is not None and prev_period != e['current_phase'] else False
            prev_period = e['current_phase']

    listeners += current_p


def add_gaps(listeners, datum_name: str):
    """
    Append gaps computation for datum_name dataset for every time moment
    :param listeners: listeners environment
    :param datum_name: the name of the DataFrame in the event. Could be bars_1m, bars_5m, etc.
    """

    def gaps_f(e):
        if datum_name in e:
            e[datum_name + '_gaps'] = gaps(e[datum_name])

    listeners += gaps_f
