import unittest

from atpy.backtesting.environments import *
from atpy.data.iqfeed.iqfeed_postgres_cache import *
from pyevents.events import SyncListeners


class TestEnvironments(unittest.TestCase):

    def test_postgre_ohlc(self):
        listeners = SyncListeners()

        dre = postgres_ohlc(listeners, include_1d=True, include_5m=True, include_1m=False, bgn_prd=datetime.datetime.now() - relativedelta(months=2), run_async=False)
        add_current_period(listeners)

        dct = {'bars_5m': 0, 'bars_1d': 0, 'latest_5m': None, 'latest_1d': None}

        def asserts(e):
            if e['type'] == 'data':
                self.assertTrue(isinstance(e, dict))

                if 'bars_5m' in e:
                    self.assertTrue(isinstance(e['bars_5m'], pd.DataFrame))
                    self.assertFalse(e['bars_5m'].empty)
                    dct['bars_5m'] += 1

                    if dct['latest_5m'] is not None:
                        self.assertGreater(e['bars_5m'].iloc[-1].name[0], dct['latest_5m'])

                    dct['latest_5m'] = e['bars_5m'].iloc[-1].name[0]

                if 'bars_1d' in e:
                    self.assertTrue(isinstance(e['bars_1d'], pd.DataFrame))
                    self.assertFalse(e['bars_1d'].empty)
                    dct['bars_1d'] += 1

                    if dct['latest_1d'] is not None:
                        self.assertGreater(e['bars_1d'].iloc[-1].name[0], dct['latest_1d'])

                    dct['latest_1d'] = e['bars_1d'].iloc[-1].name[0]

        listeners += asserts
        dre.start()

        self.assertGreater(dct['bars_5m'], 0)
        self.assertGreater(dct['bars_1d'], 0)
        self.assertIsNotNone(dct['latest_5m'])
        self.assertIsNotNone(dct['latest_1d'])

    def test_postgre_ohlc_quandl_sf0(self):
        listeners = SyncListeners()

        dre = postgres_ohlc(listeners, include_1d=True, include_5m=False, include_1m=False, bgn_prd=datetime.datetime.now() - relativedelta(years=1), run_async=False)
        add_current_period(listeners)
        add_quandl_sf(dre, bgn_prd=datetime.datetime.now() - relativedelta(years=1))

        dct = {'bars_1d': 0, 'quandl_sf0': 0, 'latest_1d': None, 'latest_quandl_sf0': None}

        def asserts(e):
            if e['type'] == 'data':
                self.assertTrue(isinstance(e, dict))

                if 'bars_1d' in e:
                    self.assertTrue(isinstance(e['bars_1d'], pd.DataFrame))
                    self.assertFalse(e['bars_1d'].empty)
                    dct['bars_1d'] += 1

                    if dct['latest_1d'] is not None:
                        self.assertGreater(e['bars_1d'].iloc[-1].name[0], dct['latest_1d'])

                    dct['latest_1d'] = e['bars_1d'].iloc[-1].name[0]

                if 'quandl_sf0' in e:
                    self.assertTrue(isinstance(e['quandl_sf0'], pd.DataFrame))
                    self.assertFalse(e['quandl_sf0'].empty)
                    dct['quandl_sf0'] += 1

                    if dct['latest_quandl_sf0'] is not None:
                        self.assertGreater(e['quandl_sf0'].iloc[-1].name[0], dct['latest_quandl_sf0'])

                    dct['latest_quandl_sf0'] = e['quandl_sf0'].iloc[-1].name[0]

        listeners += asserts
        dre.start()

        self.assertGreater(dct['bars_1d'], 0)
        self.assertGreater(dct['quandl_sf0'], 0)
        self.assertIsNotNone(dct['latest_1d'])
        self.assertIsNotNone(dct['latest_quandl_sf0'])


if __name__ == '__main__':
    unittest.main()
