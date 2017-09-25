import unittest

from atpy.data.iqfeed.iqfeed_bar_data_provider import *
from atpy.data.influxdb_cache import *
from influxdb import DataFrameClient


class TestInfluxDBCache(unittest.TestCase):
    """
    Test InfluxDBCache
    """

    def setUp(self):
        events.reset()
        events.use_global_event_bus()
        self._client = DataFrameClient('localhost', 8086, 'root', 'root')
        self._client.create_database('test_cache')
        self._client.switch_database('test_cache')

    def tearDown(self):
        self._client.drop_database('test_cache')

    def test_cache(self):
        client = self._client
        _self = self

        e = threading.Event()

        class InfluxDBCacheTest(InfluxDBCache):
            @events.listener
            def on_event(self, event):
                super().on_event(event)
                if self._use_stream_events and event['type'] == 'bar':
                    with self._lock:
                        cached = client.query("select * from bars")
                        self.update_latest_values()
                        _self.assertTrue(isinstance(cached, dict))
                        _self.assertTrue(isinstance(cached['bars'], pd.DataFrame))
                        _self.assertFalse(cached['bars'].empty)

                        symbols = list(cached['bars']['symbol'])
                        _self.assertTrue('IBM' in symbols or 'GOOG' in symbols)
                        _self.assertEqual(cached['bars']['interval'][0], '3600-s')

                        e.set()

        with IQFeedBarDataListener(mkt_snapshot_depth=3, interval_len=3600), InfluxDBCacheTest(use_stream_events=True, client=client, time_delta_back=relativedelta(days=3)):
            watch_bars = events.after(lambda: {'type': 'watch_bars', 'data': {'symbol': ['GOOG', 'IBM'], 'update': 1}})
            watch_bars()

            e.wait()


if __name__ == '__main__':
    unittest.main()
