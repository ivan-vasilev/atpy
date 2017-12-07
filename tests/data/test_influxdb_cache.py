import unittest

from atpy.data.iqfeed.iqfeed_bar_data_provider import *
from atpy.data.influxdb_cache import *
from influxdb import DataFrameClient
from pandas.util.testing import assert_frame_equal


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
        filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                   BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                   BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=600, ascend=True, interval_type='s'))

        with IQFeedHistoryProvider(exclude_nan_ratio=None, num_connections=2) as history, InfluxDBCache(use_stream_events=True, client=self._client, history_provider=history, time_delta_back=relativedelta(days=30)) as cache:
            data = [history.request_data(f, synchronize_timestamps=False, adjust_data=False) for f in filters]

            for datum, f in zip(data, filters):
                datum.drop('timestamp', axis=1, inplace=True)
                datum['interval'] = str(f.interval_len) + '_' + f.interval_type
                cache.client.write_points(datum, 'bars', protocol='line', tag_columns=['symbol', 'interval'])

            latest_old = cache.ranges
            cache.update_to_latest({'AAPL': [(3600, 's')], 'MSFT': [(3600, 's'), (600, 's')]})

        latest_current = cache.ranges
        self.assertEqual(len(latest_current), len(latest_old) + 2)
        self.assertEqual(len([k for k in latest_current.keys() & latest_old.keys()]) + 2, len(latest_current))
        for k in latest_current.keys() & latest_old.keys():
            self.assertGreater(latest_current[k][1], latest_old[k][1])

    def test_request_data(self):
        with IQFeedHistoryProvider(exclude_nan_ratio=None, num_connections=2) as history, InfluxDBCache(use_stream_events=True, client=self._client, history_provider=history, time_delta_back=relativedelta(days=3)) as cache:
            end_prd = datetime.datetime(2017, 5, 1)

            # test single symbol request
            filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=600, ascend=True, interval_type='s'))

            data = [history.request_data(f, synchronize_timestamps=False, adjust_data=False) for f in filters]

            for datum, f in zip(data, filters):
                datum.drop('timestamp', axis=1, inplace=True)
                datum['interval'] = str(f.interval_len) + '_' + f.interval_type
                cache.client.write_points(datum, 'bars', protocol='line', tag_columns=['symbol', 'interval'])
                datum.drop('interval', axis=1, inplace=True)

                test_data = cache.request_data(interval_len=f.interval_len, interval_type=f.interval_type, symbol=f.ticker)
                assert_frame_equal(datum, test_data)

            for datum, f in zip(data, filters):
                test_data_limit = cache.request_data(interval_len=f.interval_len, interval_type=f.interval_type, symbol=f.ticker, bgn_prd=f.bgn_prd + relativedelta(days=7), end_prd=f.end_prd - relativedelta(days=7))
                self.assertGreater(len(test_data_limit), 0)
                self.assertLess(len(test_data_limit), len(test_data))

            # test multisymbol request
            requested_data = history.request_data(BarsInPeriodFilter(ticker=["AAPL", "IBM"], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'), synchronize_timestamps=False, adjust_data=False)
            test_data = cache.request_data(interval_len=3600, interval_type='s', symbol=['IBM', 'AAPL'], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd)
            assert_frame_equal(requested_data, test_data)

            # test any symbol request
            requested_data = history.request_data(BarsInPeriodFilter(ticker=["AAPL", "IBM"], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'), synchronize_timestamps=False, adjust_data=False)
            test_data = cache.request_data(interval_len=3600, interval_type='s', symbol=None, bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd)
            assert_frame_equal(requested_data, test_data)

    def test_get_missing_symbols(self):
        end_prd = datetime.datetime(2017, 3, 2)
        filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                   BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                   BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=600, ascend=True, interval_type='s'))

        with IQFeedHistoryProvider(exclude_nan_ratio=None, num_connections=2) as history, InfluxDBCache(use_stream_events=True, client=self._client, history_provider=history, time_delta_back=relativedelta(days=3)) as cache:
            data = [history.request_data(f, synchronize_timestamps=False, adjust_data=False) for f in filters]

            for datum, f in zip(data, filters):
                datum.drop('timestamp', axis=1, inplace=True)
                datum['interval'] = str(f.interval_len) + '_' + f.interval_type
                cache.client.write_points(datum, 'bars', protocol='line', tag_columns=['symbol', 'interval'])

        symbols = cache.get_missing_symbols([(3600, 's'), (600, 's')])
        self.assertTrue(len(symbols) > 0)
        self.assertFalse('AAPL' in symbols)
        self.assertEqual(len(symbols['IBM']), 1)


if __name__ == '__main__':
    unittest.main()
