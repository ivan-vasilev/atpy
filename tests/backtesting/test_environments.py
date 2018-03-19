import unittest

from sqlalchemy import create_engine

from atpy.backtesting.data_replay import DataReplay, DataReplayEvents
from atpy.data.cache.postgres_cache import *
from atpy.data.iqfeed.iqfeed_postgres_cache import *
from atpy.data.ts_util import current_period
from pyevents.events import SyncListeners


class TestEnvironments(unittest.TestCase):

    def test_1(self):
        # test all symbols
        table_name = 'bars_test'

        url = 'postgresql://postgres:postgres@localhost:5432/test'

        engine = create_engine(url)
        con = psycopg2.connect(url)
        con.autocommit = True

        now = datetime.datetime.now()
        bgn_prd = datetime.datetime(now.year - 1, 3, 1, tzinfo=tz.gettz('UTC'))
        bars_in_period = BarsInPeriodProvider(conn=con, interval_len=3600, interval_type='s', bars_table=table_name, bgn_prd=bgn_prd, delta=relativedelta(months=1), overlap=relativedelta(microseconds=-1))

        cur = con.cursor()

        try:
            cur.execute(create_bars.format(table_name))
            cur.execute(bars_indices.format(table_name))

            with IQFeedHistoryProvider(num_connections=2) as history, DataReplay().add_source(iter(bars_in_period), 'bars', historical_depth=100) as dr:
                now = datetime.datetime.now()
                bgn_prd = datetime.datetime(now.year - 1, 3, 1).astimezone(tz.gettz('US/Eastern'))
                filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                           BarsInPeriodFilter(ticker="AAPL", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'))

                data = [history.request_data(f, sync_timestamps=False) for f in filters]

                for datum, f in zip(data, filters):
                    del datum['timestamp']
                    datum['symbol'] = f.ticker
                    datum['interval'] = '3600_s'
                    datum = datum.tz_localize(None)
                    datum.to_sql(table_name, con=engine, if_exists='append')

                listeners = SyncListeners()

                dre = DataReplayEvents(listeners, dr, event_name='data')

                def current_p(e):
                    if e['type'] == 'data':
                        e['current_period_df'], e['current_period'] = current_period(e['data']['bars'])

                listeners += current_p

                def unit_tests(e):
                    if e['type'] == 'data':
                        d = e['data']
                        self.assertFalse(d['bars'].empty)

                listeners += unit_tests

                dre.start()

                for i, df in enumerate(dr):
                    self.assertFalse(df['bars'].empty)

        finally:
            con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))


if __name__ == '__main__':
    unittest.main()
