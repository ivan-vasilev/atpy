import unittest

import pandas as pd

from atpy.data.intrinio.api import IntrinioEvents
from pyevents.events import SyncListeners


class TestIQFeedBarData(unittest.TestCase):

    def test_1(self):
        listeners = SyncListeners()
        IntrinioEvents(listeners)

        results = list()

        def listener(event):
            if event['type'] == 'intrinio_request_result':
                results.append(event['data'])

        listeners += listener

        listeners({'type': 'intrinio_request', 'endpoint': 'companies', 'dataframe': True, 'parameters': {'query': 'Computer'}})

        data = results[0]

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)

    def test_2(self):
        listeners = SyncListeners()
        IntrinioEvents(listeners)

        results = list()

        def listener(event):
            if event['type'] == 'intrinio_historical_result':
                results.append(event['data'])

        listeners += listener

        listeners({'type': 'intrinio_historical_request',
                   'data': [{'endpoint': 'historical_data', 'identifier': 'GOOG', 'item': 'totalrevenue'}, {'endpoint': 'historical_data', 'identifier': 'YHOO', 'item': 'totalrevenue'}],
                   'threads': 1,
                   'async': False})

        data = results[0]

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)
        self.assertTrue(isinstance(data.index, pd.MultiIndex))


if __name__ == '__main__':
    unittest.main()
