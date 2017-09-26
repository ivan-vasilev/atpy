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

    def test_streaming_cache(self):
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

    def test_update_to_latest(self):
        end_prd = datetime.datetime(2017, 3, 2)
        filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s', max_ticks=20),
                   BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s', max_ticks=20),
                   BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=600, ascend=True, interval_type='s', max_ticks=20))

        with IQFeedHistoryProvider(exclude_nan_ratio=None, num_connections=2) as history, InfluxDBCache(use_stream_events=True, client=self._client, history_provider=history, time_delta_back=relativedelta(days=3)) as cache:
            data = [history.request_data(f, synchronize_timestamps=False, adjust_data=False) for f in filters]

            for datum, f in zip(data, filters):
                datum.drop('time_stamp', axis=1, inplace=True)
                datum['interval'] = str(f.interval_len) + '-' + f.interval_type
                cache.client.write_points(datum, 'bars', protocol='line', tag_columns=['symbol', 'interval'])

            latest_old = cache.latest_entries
            cache.update_to_latest()

        latest_current = cache.latest_entries
        self.assertEqual(len(latest_current), len(latest_old))
        for t1, t2 in zip([e[0] for e in latest_current], [e[0] for e in latest_old]):
            self.assertGreater(t1, t2)


if __name__ == '__main__':
    unittest.main()
