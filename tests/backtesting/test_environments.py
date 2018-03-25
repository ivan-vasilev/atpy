import unittest

from atpy.backtesting.environments import *
from atpy.data.iqfeed.iqfeed_postgres_cache import *
from pyevents.events import SyncListeners


class TestEnvironments(unittest.TestCase):

    def test_1(self):
        listeners = SyncListeners()

        now = datetime.datetime.now()
        dre = postgres_ohlc(listeners, include_1d=True, include_5m=True, include_1m=False, bgn_prd=datetime.datetime(year=now.year - 2, month=1, day=1))
        add_current_period(listeners)
        add_iq_symbol_data(listeners, symbols_file='/home/hok/Downloads/mktsymbols_v2.zip')

        def asserts(e):
            if 'bars_5m' in e:
                print(e['bars_5m'].iloc[-1].name[0])

            if e['type'] == 'data':
                self.assertTrue(isinstance(e, dict))

        listeners += asserts
        dre.start()


if __name__ == '__main__':
    unittest.main()
