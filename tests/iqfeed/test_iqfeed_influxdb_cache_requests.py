import unittest

from pandas.util.testing import assert_frame_equal

from atpy.data.iqfeed.iqfeed_bar_data_provider import *
from atpy.data.iqfeed.iqfeed_influxdb_cache import *
from atpy.data.iqfeed.iqfeed_influxdb_cache_requests import *
from atpy.data.cache.influxdb_cache_requests import *


class TestInfluxDBCacheRequests(unittest.TestCase):
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

    def test_request_ohlc(self):
        with IQFeedHistoryProvider(exclude_nan_ratio=None, num_connections=2) as history, \
                IQFeedInfluxDBCache(use_stream_events=True, client=self._client, history=history, time_delta_back=relativedelta(days=3)) as cache:
            cache_requests = IQFeedInfluxDBOHLCRequest(client=self._client, streaming_conn=history.streaming_conn)

            end_prd = datetime.datetime(2017, 5, 1)

            # test single symbol request
            filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=600, ascend=True, interval_type='s'))

            adjusted = list()

            for f in filters:
                datum = history.request_data(f, synchronize_timestamps=False, adjust_data=False)
                datum.drop('timestamp', axis=1, inplace=True)
                datum['interval'] = str(f.interval_len) + '_' + f.interval_type
                cache.client.write_points(datum, 'bars', protocol='line', tag_columns=['symbol', 'interval'])
                datum.drop('interval', axis=1, inplace=True)

                datum = history.request_data(f, synchronize_timestamps=False, adjust_data=True)
                adjusted.append(datum)
                test_data = cache_requests.request(interval_len=f.interval_len, interval_type=f.interval_type, symbol=f.ticker, adjust_data=True)
                assert_frame_equal(datum, test_data)

            for datum, f in zip(adjusted, filters):
                test_data_limit = cache_requests.request(interval_len=f.interval_len, interval_type=f.interval_type, symbol=f.ticker, bgn_prd=f.bgn_prd + relativedelta(days=7), end_prd=f.end_prd - relativedelta(days=7), adjust_data=True)
                self.assertGreater(len(test_data_limit), 0)
                self.assertLess(len(test_data_limit), len(test_data))

            # test multisymbol request
            requested_data = history.request_data(BarsInPeriodFilter(ticker=["AAPL", "IBM"], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'), synchronize_timestamps=False, adjust_data=True)
            test_data = cache_requests.request(interval_len=3600, interval_type='s', symbol=['IBM', 'AAPL', 'TSG'], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, adjust_data=True)
            assert_frame_equal(requested_data, test_data)

            # test any symbol request
            requested_data = history.request_data(BarsInPeriodFilter(ticker=["AAPL", "IBM"], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'), synchronize_timestamps=False, adjust_data=True)

            e = threading.Event()

            @events.listener
            def listen(event):
                if event['type'] == 'cache_result':
                    assert_frame_equal(requested_data, event['data'])
                    e.set()

            cache_requests.on_event({'type': 'request_ohlc', 'data': {'interval_len': 3600, 'interval_type': 's', 'bgn_prd': datetime.datetime(2017, 4, 1), 'end_prd': end_prd, 'adjust_data': True}})

            e.wait()

    def test_request_deltas(self):
        with IQFeedHistoryProvider(exclude_nan_ratio=None, num_connections=2) as history, \
                IQFeedInfluxDBCache(use_stream_events=True, client=self._client, history=history, time_delta_back=relativedelta(days=3)) as cache:
            cache_requests = InfluxDBDeltaRequest(client=self._client)

            end_prd = datetime.datetime(2017, 5, 1)

            # test single symbol request
            filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=600, ascend=True, interval_type='s'))

            for f in filters:
                data = history.request_data(f, synchronize_timestamps=False, adjust_data=False)
                data.drop('timestamp', axis=1, inplace=True)
                data['interval'] = str(f.interval_len) + '_' + f.interval_type
                cache.client.write_points(data, 'bars', protocol='line', tag_columns=['symbol', 'interval'])

                delta = (data['close'] - data['open']) / data['open']
                test_delta = cache_requests.request(interval_len=f.interval_len, interval_type=f.interval_type, symbol=f.ticker)['delta']
                self.assertFalse(False in delta == test_delta)

            # test multisymbol request
            requested_data = history.request_data(BarsInPeriodFilter(ticker=["AAPL", "IBM"], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'), synchronize_timestamps=False, adjust_data=False)
            delta = (requested_data['close'] - requested_data['open']) / requested_data['open']

            test_delta = cache_requests.request(interval_len=3600, interval_type='s', symbol=['IBM', 'AAPL', 'TSG'], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd)['delta']
            self.assertFalse(False in delta == test_delta)

            # test any symbol request
            requested_data = history.request_data(BarsInPeriodFilter(ticker=["AAPL", "IBM"], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'), synchronize_timestamps=False, adjust_data=False)
            delta = (requested_data['close'] - requested_data['open']) / requested_data['open']

            e = threading.Event()

            @events.listener
            def listen(event):
                if event['type'] == 'cache_result':
                    self.assertFalse(False in delta == event['data']['delta'])
                    e.set()

            cache_requests.on_event({'type': 'request_delta', 'data': {'interval_len': 3600, 'interval_type': 's', 'bgn_prd': datetime.datetime(2017, 4, 1), 'end_prd': end_prd}})

            e.wait()


if __name__ == '__main__':
    unittest.main()
