import unittest

import pandas as pd
import datetime

from atpy.data.intrinio.api import IntrinioEvents
from pyevents.events import SyncListeners
from atpy.data.intrinio.influxdb_cache import InfluxDBCache, ClientFactory


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

    def test_3(self):
        listeners = SyncListeners()
        IntrinioEvents(listeners)

        client_factory = ClientFactory(host='localhost', port=8086, username='root', password='root', database='test_cache')
        client = client_factory.new_df_client()

        try:
            client.create_database('test_cache')
            client.switch_database('test_cache')

            with InfluxDBCache(client_factory=client_factory, listeners=listeners) as cache:
                cache.update_to_latest({('GOOG', 'operatingrevenue'), ('FB', 'operatingrevenue'), ('YHOO', 'operatingrevenue')})
                now = datetime.datetime.now()
                cached = cache.request_data(symbols={'GOOG', 'MSFT', 'YHOO', 'FB'}, tags={'operatingrevenue'}, start_date=datetime.date(year=now.year - 4, month=now.month, day=now.day), end_date=datetime.date(year=now.year - 2, month=now.month, day=now.day))

            self.assertIsNotNone(cached)
            self.assertGreater(len(cached), 0)
            self.assertGreaterEqual(now.year - 2, cached.index.levels[0].max().year)
            self.assertGreaterEqual(cached.index.levels[0].min().year, now.year - 4)
        finally:
            client.drop_database('test_cache')
            client.close()


if __name__ == '__main__':
    unittest.main()
