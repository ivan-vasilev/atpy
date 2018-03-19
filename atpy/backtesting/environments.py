import datetime

import psycopg2

from atpy.backtesting.data_replay import DataReplayEvents
from atpy.data.cache.postgres_cache import BarsInPeriodProvider


def postgres_ohlc(listeners, url: str, table_name: str, bgn_prd: datetime.datetime, current_period: bool, symbol_data: bool, fundamentals: bool):
    con = psycopg2.connect(url)

    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=3600, interval_type='s', bars_table=table_name, bgn_prd=bgn_prd, delta=relativedelta(months=1), overlap=relativedelta(microseconds=-1))

    dre = DataReplayEvents(listeners, dr, event_name='data')

    def current_p(e):
        if e['type'] == 'data':
            e['current_period_df'], e['current_period'] = current_period(e['data']['bars'])

    listeners += current_p

    def unit_tests(e):
        if e['type'] == 'data':
            d = e['data']
            self.assertFalse(d['bars'].empty)
