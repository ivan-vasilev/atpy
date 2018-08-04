import threading
import unittest

from pandas.util.testing import assert_frame_equal

from atpy.data.cache.influxdb_cache_requests import *
from atpy.data.iqfeed.iqfeed_bar_data_provider import *
from atpy.data.iqfeed.iqfeed_influxdb_cache import *
from atpy.data.iqfeed.iqfeed_level_1_provider import get_fundamentals
from pyevents.events import AsyncListeners
from atpy.data.splits_dividends import adjust_df


class TestInfluxDBCacheRequests(unittest.TestCase):
    """
    Test InfluxDBCache
    """

    def setUp(self):
        self._client = DataFrameClient(host='localhost', port=8086, username='root', password='root', database='test_cache')

        self._client.drop_database('test_cache')
        self._client.create_database('test_cache')
        self._client.switch_database('test_cache')

    def tearDown(self):
        self._client.drop_database('test_cache')
        self._client.close()

    def test_request_ohlc(self):
        listeners = AsyncListeners()

        with IQFeedHistoryProvider(num_connections=2) as history:
            streaming_conn = iq.QuoteConn()
            streaming_conn.connect()

            end_prd = datetime.datetime(2017, 5, 1)

            # test single symbol request
            filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=600, ascend=True, interval_type='s'))

            update_splits_dividends(client=self._client, fundamentals=get_fundamentals({'IBM', 'AAPL'}, streaming_conn).values())
            adjusted = list()

            for f in filters:
                datum = history.request_data(f, sync_timestamps=False)
                datum.drop('timestamp', axis=1, inplace=True)
                datum['interval'] = str(f.interval_len) + '_' + f.interval_type
                self._client.write_points(datum, 'bars', protocol='line', tag_columns=['symbol', 'interval'], time_precision='s')
                datum.drop('interval', axis=1, inplace=True)

                datum = history.request_data(f, sync_timestamps=False)

                adjust_df(datum, get_adjustments(client=self._client, symbol=f.ticker))
                adjusted.append(datum)

                cache_requests = InfluxDBOHLCRequest(client=self._client, interval_len=f.interval_len, interval_type=f.interval_type)
                _, test_data = cache_requests.request(symbol=f.ticker)
                adjust_df(test_data, get_adjustments(client=self._client, symbol=f.ticker))
                del datum['total_volume']
                del datum['number_of_trades']
                assert_frame_equal(datum, test_data)

            for datum, f in zip(adjusted, filters):
                cache_requests = InfluxDBOHLCRequest(client=self._client, interval_len=f.interval_len, interval_type=f.interval_type)
                _, test_data = cache_requests.request(symbol=f.ticker)
                _, test_data_limit = cache_requests.request(symbol=f.ticker, bgn_prd=f.bgn_prd + relativedelta(days=7), end_prd=f.end_prd - relativedelta(days=7))

                self.assertGreater(len(test_data_limit), 0)
                self.assertLess(len(test_data_limit), len(test_data))

            # test multisymbol request
            requested_data = history.request_data(BarsInPeriodFilter(ticker=["AAPL", "IBM"], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'), sync_timestamps=False)
            requested_data = requested_data.swaplevel(0, 1).sort_index()
            del requested_data['total_volume']
            del requested_data['number_of_trades']

            cache_requests = InfluxDBOHLCRequest(client=self._client, interval_len=3600, listeners=listeners)
            _, test_data = cache_requests.request(symbol=['IBM', 'AAPL', 'TSG'], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd)
            assert_frame_equal(requested_data, test_data)

            # test any symbol request
            requested_data = history.request_data(BarsInPeriodFilter(ticker=["AAPL", "IBM"], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'), sync_timestamps=False)
            requested_data = requested_data.swaplevel(0, 1).sort_index()

            del requested_data['total_volume']
            del requested_data['number_of_trades']

            e = threading.Event()

            def listen(event):
                if event['type'] == 'cache_result':
                    assert_frame_equal(requested_data, event['data'][0])
                    e.set()

            listeners += listen

            listeners({'type': 'request_ohlc', 'data': {'bgn_prd': datetime.datetime(2017, 4, 1), 'end_prd': end_prd}})

            e.wait()

            streaming_conn.disconnect()


if __name__ == '__main__':
    unittest.main()
