import unittest

import pandas as pd

from atpy.data.quandl.api import QuandlEvents, get_sf, bulkdownload_sf
from atpy.data.quandl.influxdb_cache import InfluxDBCache
from pyevents.events import SyncListeners
from influxdb import DataFrameClient
from pandas.util.testing import assert_frame_equal


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
        for data in bulkdownload_sf():
            called = True
            self.assertTrue(isinstance(data, pd.DataFrame))
            self.assertGreater(len(data), 0)
            self.assertEqual(len(data.index.levels), 4)

        self.assertTrue(called)

    def test_cache(self):
        client = DataFrameClient(host='localhost', port=8086, username='root', password='root', database='test_cache')

        try:
            client.drop_database('test_cache')
            client.create_database('test_cache')
            client.switch_database('test_cache')

            with InfluxDBCache(client=client) as cache:
                cache.add_sf_to_cache()
                data = cache.request_data('SF0', tags={'symbol': {'AAPL', 'IBM'}})

                listeners = SyncListeners()
                QuandlEvents(listeners)

                self.assertTrue(isinstance(data, pd.DataFrame))
                self.assertGreater(len(data), 0)
                self.assertEqual(len(data.index.levels), 4)

                non_cache_data = get_sf([{'dataset': 'SF0/NKE_GP_MRY'}, {'dataset': 'SF0/MSFT_GP_MRY'}])
                cache_data = cache.request_data('SF0', tags={'symbol': {'NKE', 'MSFT'}, 'dimension': 'MRY', 'indicator': 'GP'})
                assert_frame_equal(non_cache_data, cache_data)
        finally:
            client.drop_database('test_cache')
            client.close()


if __name__ == '__main__':
    unittest.main()
