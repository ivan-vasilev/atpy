import unittest

import pandas as pd

from atpy.data.quandl.api import QuandlEvents, get_core_us_fund_from_bulk
from pyevents.events import SyncListeners


class TestQuandlAPI(unittest.TestCase):

    def test_1(self):
        listeners = SyncListeners()
        QuandlEvents(listeners)

        results = list()

        def listener(event):
            if event['type'] == 'quandl_timeseries_result':
                results.append(event['data'])

        listeners += listener

        listeners({'type': 'quandl_fundamentals_request',
                   'data': [{'dataset': 'SF1/NKE_GP_MRQ'}, {'dataset': 'SF1/AAPL_GP_MRQ'}],
                   'threads': 1,
                   'async': False})

        data = results[0]

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)
        self.assertEqual(len(data.index.levels), 4)

        listeners({'type': 'quandl_fundamentals_request',
                   'data': [{'dataset': 'SF1/NKE_GP_MRQ'}],
                   'threads': 1,
                   'async': False})

        data = results[0]

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)
        self.assertEqual(len(data.index.levels), 4)

    def test_bulkdownload(self):
        called = False
        for data in get_core_us_fund_from_bulk():
            called = True
            self.assertTrue(isinstance(data, pd.DataFrame))
            self.assertGreater(len(data), 0)
            self.assertEqual(len(data.index.levels), 4)

        self.assertTrue(called)


if __name__ == '__main__':
    unittest.main()
