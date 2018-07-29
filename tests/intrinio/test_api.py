import datetime
import unittest

import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas.util.testing import assert_frame_equal

from atpy.data.intrinio.api import IntrinioEvents
from atpy.data.intrinio.influxdb_cache import InfluxDBCache, ClientFactory
from pyevents.events import SyncListeners


class TestIntrinioAPI(unittest.TestCase):

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
            if event['type'] == 'intrinio_historical_data_result':
                results.append(event['data'])

        listeners += listener

        listeners({'type': 'intrinio_historical_data',
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
        client = client_factory.new_client()

        try:
            client.create_database('test_cache')
            client.switch_database('test_cache')

            with InfluxDBCache(client_factory=client_factory, listeners=listeners, time_delta_back=relativedelta(years=20)) as cache:
                cache.update_to_latest({('GOOG', 'operatingrevenue'), ('FB', 'operatingrevenue'), ('YHOO', 'operatingrevenue')})
                now = datetime.datetime.now()
                cached = cache.request_data(symbols={'GOOG', 'MSFT', 'YHOO', 'FB'}, tags={'operatingrevenue'}, start_date=datetime.date(year=now.year - 4, month=now.month, day=now.day),
                                            end_date=datetime.date(year=now.year - 2, month=now.month, day=now.day))

                self.assertIsNotNone(cached)
                self.assertGreater(len(cached), 0)
                self.assertGreaterEqual(now.year - 2, cached.index.levels[1].max().year)
                self.assertGreaterEqual(cached.index.levels[1].min().year, now.year - 4)

                cached = cache.request_data(symbols={'GOOG', 'FB'}, tags={'operatingrevenue'})

                listeners = SyncListeners()
                IntrinioEvents(listeners)

                non_cached = list()

                def listener(event):
                    if event['type'] == 'intrinio_historical_data_result':
                        non_cached.append(event['data'])

                listeners += listener

                listeners({'type': 'intrinio_historical_data',
                           'data': [{'endpoint': 'historical_data', 'identifier': 'GOOG', 'item': 'operatingrevenue', 'sort_order': 'asc'},
                                    {'endpoint': 'historical_data', 'identifier': 'FB', 'item': 'operatingrevenue', 'sort_order': 'asc'}],
                           'async': False})

                non_cached = non_cached[0]

                assert_frame_equal(cached, non_cached)
        finally:
            client.drop_database('test_cache')
            client.close()


if __name__ == '__main__':
    unittest.main()
