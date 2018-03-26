import unittest

from atpy.backtesting.environments import *
from atpy.data.iqfeed.iqfeed_postgres_cache import *
from pyevents.events import SyncListeners


class TestEnvironments(unittest.TestCase):

    def test_1(self):
        listeners = SyncListeners()

        now = datetime.datetime.now()
        dre = postgres_ohlc(listeners, include_1d=False, include_5m=True, include_1m=False, bgn_prd=datetime.datetime(year=now.year - 2, month=1, day=1), run_async=False)
        # add_current_period(listeners)
        # add_iq_symbol_data(listeners, symbols_file='/home/hok/Downloads/mktsymbols_v2.zip')

        def asserts(e):
            if 'bars_5m' in e:
                print(e['bars_5m'].iloc[-1].name[0])

            # if e['type'] == 'data':
            #     self.assertTrue(isinstance(e, dict))

        listeners += asserts
        dre.start()

    def test_2(self):
        con = psycopg2.connect(os.environ['POSTGRESQL_CACHE'])

        now = datetime.datetime.now()
        bgn_prd = datetime.datetime(year=now.year - 2, month=1, day=1)
        bars_in_period = BarsInPeriodProvider(conn=con, interval_len=300, interval_type='s', bars_table='bars_5m', bgn_prd=bgn_prd, delta=relativedelta(months=1), overlap=relativedelta(microseconds=-1))

        for df in bars_in_period:
            print(df.shape)

    def test_3(self):
        con = psycopg2.connect(os.environ['POSTGRESQL_CACHE'])

        now = datetime.datetime.now()
        bgn_prd = datetime.datetime(year=now.year - 2, month=1, day=1)
        bars_in_period = BarsInPeriodProvider(conn=con, interval_len=300, interval_type='s', bars_table='bars_5m', bgn_prd=bgn_prd, delta=relativedelta(months=1), overlap=relativedelta(microseconds=-1))

        dr = DataReplay()

        dr.add_source(bars_in_period, 'bars_5m', historical_depth=300)

        for df in dr:
            print(df['bars_5m'].iloc[-1].name[0])


if __name__ == '__main__':
    unittest.main()
