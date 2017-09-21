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
        self._client.drop_database('test_cache')
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
                        _self.assertIsNone(cached.error)

                        for p in cached.get_points(measurement='bars'):
                            _self.assertEqual(len(p), 11)
                            _self.assertTrue(p['symbol'] in ['IBM', 'GOOG'])

                        e.set()

        with IQFeedBarDataListener(mkt_snapshot_depth=3, interval_len=300), InfluxDBCacheTest(use_stream_events=True, client=client, time_delta_back=relativedelta(days=10)):
            watch_bars = events.after(lambda: {'type': 'watch_bars', 'data': {'symbol': ['GOOG', 'IBM'], 'update': 1}})
            watch_bars()

            e.wait()


if __name__ == '__main__':
    unittest.main()
