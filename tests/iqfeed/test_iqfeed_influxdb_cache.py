import unittest

from atpy.data.iqfeed.iqfeed_level_1_provider import get_fundamentals
from dateutil.relativedelta import relativedelta
from influxdb import DataFrameClient
from pandas.util.testing import assert_frame_equal

import atpy.data.cache.influxdb_cache_requests as inf_cache
from atpy.data.cache.influxdb_cache import update_to_latest, ranges
from atpy.data.iqfeed.iqfeed_bar_data_provider import *
from atpy.data.iqfeed.iqfeed_influxdb_cache import *
from atpy.data.iqfeed.iqfeed_influxdb_cache_requests import get_cache_fundamentals


class TestInfluxDBCache(unittest.TestCase):
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

    def test_update_to_latest(self):
        with IQFeedHistoryProvider(num_connections=2) as history:
            cache_requests = inf_cache.InfluxDBOHLCRequest(client=self._client, interval_len=3600, interval_type='s')

            end_prd = datetime.datetime(2017, 3, 2)
            filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=end_prd, interval_len=600, ascend=True, interval_type='s'))

            filters_no_limit = (BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                                BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(2017, 3, 1), end_prd=None, interval_len=3600, ascend=True, interval_type='s'))

            data = [history.request_data(f, sync_timestamps=False) for f in filters]

            for datum, f in zip(data, filters):
                datum.drop('timestamp', axis=1, inplace=True)
                datum['interval'] = str(f.interval_len) + '_' + f.interval_type
                self._client.write_points(datum, 'bars', protocol='line', tag_columns=['symbol', 'interval'], time_precision='s')

            latest_old = ranges(self._client)
            update_to_latest(self._client, noncache_provider=noncache_provider(history), new_symbols={('AAPL', 3600, 's'), ('MSFT', 3600, 's'), ('MSFT', 600, 's')}, time_delta_back=relativedelta(days=30))

            latest_current = ranges(self._client)
            self.assertEqual(len(latest_current), len(latest_old) + 2)
            self.assertEqual(len([k for k in latest_current.keys() & latest_old.keys()]) + 2, len(latest_current))
            for k in latest_current.keys() & latest_old.keys():
                self.assertGreater(latest_current[k][1], latest_old[k][1])

            data_no_limit = [history.request_data(f, sync_timestamps=False) for f in filters_no_limit]
            cache_data_no_limit = [cache_requests.request(symbol=f.ticker, bgn_prd=f.bgn_prd)[0] for f in filters_no_limit]
            for df1, df2 in zip(data_no_limit, cache_data_no_limit):
                del df1['period_volume']
                del df1['total_volume']
                del df2['period_volume']
                del df2['total_volume']

                assert_frame_equal(df1, df2)

    def test_bars_in_period(self):
        with IQFeedHistoryProvider(num_connections=2) as history:
            now = datetime.datetime.now()
            filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(now.year - 1, 3, 1), end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(now.year - 1, 3, 1), end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                       BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(now.year - 1, 3, 1), end_prd=None, interval_len=600, ascend=True, interval_type='s'))

            data = [history.request_data(f, sync_timestamps=False) for f in filters]

            for datum, f in zip(data, filters):
                datum.drop('timestamp', axis=1, inplace=True)
                datum['interval'] = str(f.interval_len) + '_' + f.interval_type
                self._client.write_points(datum, 'bars', protocol='line', tag_columns=['symbol', 'interval'], time_precision='s')

            # test all symbols
            bgn_prd = datetime.datetime(now.year - 1, 3, 1, tzinfo=tz.gettz('UTC'))
            bars_in_period = inf_cache.BarsInPeriodProvider(influxdb_cache=inf_cache.InfluxDBOHLCRequest(client=self._client, interval_len=3600, interval_type='s'), bgn_prd=bgn_prd, delta=datetime.timedelta(days=30))

            for i, (orig_df, processed_df) in enumerate(bars_in_period):
                self.assertFalse(orig_df.empty)
                self.assertFalse(processed_df.empty)

                start, end = bars_in_period._periods[bars_in_period._deltas]
                self.assertGreaterEqual(orig_df.iloc[0].name[0], start)
                self.assertGreater(end, orig_df.iloc[-1].name[0])
                self.assertGreater(end, orig_df.iloc[0].name[0])

            self.assertEqual(i, len(bars_in_period._periods) - 1)
            self.assertGreater(i, 0)

            # test symbols group
            bgn_prd = datetime.datetime(now.year - 1, 3, 1, tzinfo=tz.gettz('UTC'))
            bars_in_period = inf_cache.BarsInPeriodProvider(influxdb_cache=inf_cache.InfluxDBOHLCRequest(client=self._client, interval_len=3600, interval_type='s'), symbol=['AAPL', 'IBM'], bgn_prd=bgn_prd, delta=datetime.timedelta(days=30))

            for i, (orig_df, processed_df) in enumerate(bars_in_period):
                self.assertFalse(orig_df.empty)
                self.assertFalse(processed_df.empty)

                start, end = bars_in_period._periods[bars_in_period._deltas]
                self.assertGreaterEqual(orig_df.iloc[0].name[0], start)
                self.assertGreater(end, orig_df.iloc[-1].name[0])
                self.assertGreater(end, orig_df.iloc[0].name[0])

            self.assertEqual(i, len(bars_in_period._periods) - 1)
            self.assertGreater(i, 0)

    def test_update_fundamentals(self):
        funds = get_fundamentals({'IBM', 'AAPL', 'GOOG', 'MSFT'})
        update_fundamentals(self._client, list(funds.values()))
        result = get_cache_fundamentals(self._client, ['IBM', 'AAPL', 'GOOG', 'MSFT'])

        self.assertEqual(len(result), 4)
        self.assertEqual({k for k in result.keys()}, {'IBM', 'AAPL', 'GOOG', 'MSFT'})
        self.assertGreater(len(result['IBM']), 0)

    def test_update_adjustments(self):
        funds = get_fundamentals({'IBM', 'AAPL', 'GOOG', 'MSFT'})
        update_splits_dividends(self._client, list(funds.values()))

        adjustments = inf_cache.get_adjustments(client=self._client, symbol=['IBM', 'AAPL'], provider='iqfeed')

        self.assertEqual(len(adjustments), 6)
        self.assertTrue(isinstance(adjustments, pd.DataFrame))
        self.assertTrue(set(adjustments.index.levels[1]) == {'IBM', 'AAPL'})


if __name__ == '__main__':
    unittest.main()
