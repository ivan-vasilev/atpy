import datetime
import os

import psycopg2
from dateutil.relativedelta import relativedelta

import atpy.data.iqfeed.util as iqutil
from atpy.backtesting.data_replay import DataReplayEvents, DataReplay
from atpy.data.cache.postgres_cache import BarsInPeriodProvider
from atpy.data.quandl.postgres_cache import SFInPeriodProvider
from atpy.data.ts_util import current_period, current_phase, gaps, AsyncInPeriodProvider


def postgres_ohlc(listeners, include_1m: bool, include_5m: bool, include_60m: bool, include_1d: bool, bgn_prd: datetime.datetime, run_async=False, url: str = None):
    """
    Create DataReplay environment for bar data using PostgreSQL
    :param listeners: listeners environment
    :param include_1m: include 1 minute data
    :param include_5m: include 5 minute data
    :param include_60m: include 60 minute data
    :param include_1d: include daily data
    :param run_async: generate data asynchronously
    :param url: postgre url (can be obtained via env variable)
    :return: dataframe
    """

    con = psycopg2.connect(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    dr = DataReplay()
    dre = DataReplayEvents(listeners, dr, event_name='data')

    if include_1m:
        bars_in_period = BarsInPeriodProvider(conn=con, interval_len=60, interval_type='s', bars_table='bars_1m', bgn_prd=bgn_prd, delta=relativedelta(months=1), overlap=relativedelta(microseconds=-1))
        if run_async:
            bars_in_period = AsyncInPeriodProvider(bars_in_period)

        dr.add_source(bars_in_period, 'bars_1m', historical_depth=300)

    if include_5m:
        bars_in_period = BarsInPeriodProvider(conn=con, interval_len=300, interval_type='s', bars_table='bars_5m', bgn_prd=bgn_prd, delta=relativedelta(months=1), overlap=relativedelta(microseconds=-1))
        if run_async:
            bars_in_period = AsyncInPeriodProvider(bars_in_period)

        dr.add_source(bars_in_period, 'bars_5m', historical_depth=200)

    if include_60m:
        bars_in_period = BarsInPeriodProvider(conn=con, interval_len=3600, interval_type='s', bars_table='bars_60m', bgn_prd=bgn_prd, delta=relativedelta(months=1), overlap=relativedelta(microseconds=-1))
        if run_async:
            bars_in_period = AsyncInPeriodProvider(bars_in_period)

        dr.add_source(bars_in_period, 'bars_60m', historical_depth=300)

    if include_1d:
        bars_in_period = BarsInPeriodProvider(conn=con, interval_len=1, interval_type='d', bars_table='bars_1d', bgn_prd=bgn_prd, delta=relativedelta(months=1), overlap=relativedelta(microseconds=-1))
        if run_async:
            bars_in_period = AsyncInPeriodProvider(bars_in_period)

        dr.add_source(bars_in_period, 'bars_1d', historical_depth=200)

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


def add_current_period(listeners, datum_name: str):
    """
    Append only current period (trading/after-hours) for the event data
    :param listeners: listeners environment
    :param datum_name: the name of the DataFrame in the event. Could be bars_1m, bars_5m, etc.
    """

    prev_period = None

    def current_p(e):
        nonlocal prev_period

        if datum_name in e:
            e[datum_name + '_current_phase'], e['current_phase'] = current_period(e[datum_name])
        else:
            e['current_phase'] = current_phase(e['timestamp'])

        e['phase_start'] = True if prev_period is not None and prev_period != e['current_phase'] else False
        prev_period = e['current_phase']

    listeners += current_p


def add_current_phase(listeners):
    """
    Append current phase
    :param listeners: listeners environment
    """

    prev_period = None

    def current_p(e):
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
