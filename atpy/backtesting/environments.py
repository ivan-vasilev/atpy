import datetime
import os

import psycopg2
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine

import atpy.data.iqfeed.util as iqutil
from atpy.backtesting.data_replay import DataReplayEvents, DataReplay
from atpy.data.cache.postgres_cache import BarsInPeriodProvider
from atpy.data.quandl.postgres_cache import SFInPeriodProvider
from atpy.data.ts_util import current_period, AsyncInPeriodProvider


def postgres_ohlc(listeners, include_1m: bool, include_5m: bool, include_1d: bool, bgn_prd: datetime.datetime, run_async=False, url: str = None):
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

    if include_1d:
        bars_in_period = BarsInPeriodProvider(conn=con, interval_len=1, interval_type='d', bars_table='bars_1d', bgn_prd=bgn_prd, delta=relativedelta(years=1), overlap=relativedelta(microseconds=-1))
        if run_async:
            bars_in_period = AsyncInPeriodProvider(bars_in_period)

        dr.add_source(bars_in_period, 'bars_1d', historical_depth=200)

    return dre


def add_current_period(listeners):
    def current_p(e):
        for k in [k for k in e if k.startswith('bars_') and k.endswith('m')]:
            e[k + '_current_period'], e['current_period'] = current_period(e[k])

    listeners += current_p


_symbols = None


def add_iq_symbol_data(listeners, symbols_file: str = None):
    global _symbols
    _symbols = iqutil.get_symbols(symbols_file=symbols_file)

    def iq_symbol_data(e):
        if e['type'] == 'data':
            e['iq_symbol_data'] = _symbols

    listeners += iq_symbol_data


def add_quandl_sf(dre: DataReplayEvents, bgn_prd: datetime.datetime, dataset_name: str = 'SF0', url: str = None):
    engine = create_engine(url if url is not None else os.environ['POSTGRESQL_CACHE'])

    name = 'quandl_' + dataset_name
    sf_in_period = SFInPeriodProvider(conn=engine, bgn_prd=bgn_prd, delta=relativedelta(years=1), overlap=relativedelta(microseconds=-1), table_name=name)
    dre.data_replay.add_source(sf_in_period, name=name, historical_depth=200)
