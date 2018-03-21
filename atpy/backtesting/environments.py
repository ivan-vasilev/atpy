import datetime

import psycopg2
from dateutil.relativedelta import relativedelta

import atpy.data.iqfeed.util as iqutil
from atpy.backtesting.data_replay import DataReplayEvents, DataReplay
from atpy.data.cache.postgres_cache import BarsInPeriodProvider
from atpy.data.ts_util import current_period

_symbols = None


def postgres_ohlc(listeners, url: str, table_5m: str, table_1m: str, table_d: str, bgn_prd: datetime.datetime, cur_period: bool, symbol_data: bool, symbols_file: str = None):
    con = psycopg2.connect(url)

    dr = DataReplay()
    dre = DataReplayEvents(listeners, dr, event_name='data')

    if table_1m is not None:
        bars_in_period = BarsInPeriodProvider(conn=con, interval_len=60, interval_type='s', bars_table=table_1m, bgn_prd=bgn_prd, delta=relativedelta(months=1), overlap=relativedelta(microseconds=-1))
        dr.add_source(iter(bars_in_period), table_1m, historical_depth=100)
        if cur_period:
            def current_p(e):
                if e['type'] == 'data':
                    e['current_period_df'], e['current_period'] = current_period(e['bars'])

            listeners += current_p

    if table_5m is not None:
        bars_in_period = BarsInPeriodProvider(conn=con, interval_len=300, interval_type='s', bars_table=table_5m, bgn_prd=bgn_prd, delta=relativedelta(months=1), overlap=relativedelta(microseconds=-1))
        dr.add_source(iter(bars_in_period), table_5m, historical_depth=100)

        if cur_period:
            def current_p(e):
                if e['type'] == 'data':
                    e['current_period_df'], e['current_period'] = current_period(e['bars'])

            listeners += current_p

    if symbol_data:
        global _symbols
        _symbols = iqutil.get_symbols(symbols_file=symbols_file)

        def symbols_data(e):
            if e['type'] == 'data':
                e['symbols_info'] = _symbols

        listeners += symbols_data

    return dre
