import random
import unittest

from pandas.util.testing import assert_frame_equal

from atpy.data.iqfeed.iqfeed_history_provider import *
from atpy.ml.util import *


class TestDataUtil(unittest.TestCase):

    def test_daily_volatility_1(self):
        batch_len = 100

        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=batch_len))
            result = daily_volatility(df['close'], parallel=False)
            self.assertTrue(isinstance(result.index, pd.DatetimeIndex))
            self.assertGreater(result.size, 0)

            df = provider.request_data(BarsFilter(ticker=["AAPL", "IBM"], interval_len=3600, interval_type='s', max_bars=batch_len))
            result_np = daily_volatility(df['close'], parallel=False)
            self.assertTrue(isinstance(result_np.index, pd.MultiIndex))
            self.assertEqual(result_np.index.names, ['symbol', 'timestamp'])
            self.assertGreater(result_np.size, 0)

            result_np.sort_index(inplace=True)
            result_p = daily_volatility(df['close'], parallel=True)
            result_p.sort_index(inplace=True)
            assert_frame_equal(result_np.to_frame(), result_p.to_frame())

            df = df.reorder_levels(['timestamp', 'symbol']).sort_index()
            result_np = daily_volatility(df['close'], parallel=False)
            self.assertTrue(isinstance(result_np.index, pd.MultiIndex))
            self.assertEqual(result_np.index.names, ['timestamp', 'symbol'])
            self.assertGreater(result_np.size, 0)

            result_np.sort_index(inplace=True)
            result_p = daily_volatility(df['close'], parallel=True)
            result_p.sort_index(inplace=True)
            assert_frame_equal(result_np.to_frame(), result_p.to_frame())

    def test_vertical_barrier_1(self):
        batch_len = 100

        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len))
            result = vertical_barrier(df.index, df.index, pd.Timedelta('5m'))
            self.assertEqual(len(result.levels), 2)
            self.assertTrue(isinstance(result, pd.MultiIndex))
            self.assertGreater(result.size, 0)

            df = provider.request_data(BarsFilter(ticker=["AAPL", "IBM"], interval_len=60, interval_type='s', max_bars=batch_len))
            result = vertical_barrier(df.index, df.index, pd.Timedelta('5m'))
            self.assertTrue(isinstance(result, pd.MultiIndex))
            self.assertEqual(len(result.levels), 3)
            self.assertEqual(result.names, ['symbol', 'timestamp', 't_end'])
            self.assertGreater(result.size, 0)

            df = df.reorder_levels(['timestamp', 'symbol']).sort_index()
            result = vertical_barrier(df.index, df.index, pd.Timedelta('5m'))
            self.assertTrue(isinstance(result, pd.MultiIndex))
            self.assertEqual(len(result.levels), 3)
            self.assertEqual(result.names, ['timestamp', 't_end', 'symbol'])
            self.assertGreater(result.size, 0)

    def test_cumsum_filter_1(self):
        batch_len = 100

        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len))
            df = df['close'].to_frame()
            df['threshold'] = 0.01

            result = cumsum_filter(df)
            self.assertTrue(isinstance(result, pd.DatetimeIndex))
            self.assertGreater(result.size, 0)

            df = provider.request_data(BarsFilter(ticker=["AAPL", "IBM"], interval_len=60, interval_type='s', max_bars=batch_len))
            df = df['close'].to_frame()
            df['threshold'] = 0.01

            result_np = cumsum_filter(df, parallel=False)
            self.assertTrue(isinstance(result_np, pd.MultiIndex))
            self.assertEqual(len(result_np.levels), 2)
            self.assertEqual(result_np.names, ['symbol', 'timestamp'])
            self.assertGreater(result_np.size, 0)

            result_p = cumsum_filter(df, parallel=True)
            assert_frame_equal(pd.DataFrame(index=result_np), pd.DataFrame(index=result_p))

            df = df.reorder_levels(['timestamp', 'symbol']).sort_index()
            result_np = cumsum_filter(df, parallel=False)

            self.assertTrue(isinstance(result_np, pd.MultiIndex))
            self.assertEqual(len(result_np.levels), 2)
            self.assertEqual(result_np.names, ['timestamp', 'symbol'])
            self.assertGreater(result_np.size, 0)

            result_p = cumsum_filter(df, parallel=True)
            assert_frame_equal(pd.DataFrame(index=result_np), pd.DataFrame(index=result_p))

    def test_triple_barriers_1(self):
        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df['pt'] = 0.001
            df['sl'] = 0.001

            result = triple_barriers(df['close'], df['pt'], sl=df['sl'], vb=pd.Timedelta('36000s'), parallel=False)
            self.assertTrue(result['pt'].isnull().any())
            self.assertTrue(result['pt'].notnull().any())
            self.assertFalse((result['pt'][result['pt'].notnull()] == result.index[result['pt'].notnull()]).any())

            self.assertTrue(result['sl'].isnull().any())
            self.assertTrue(result['sl'].notnull().any())
            self.assertFalse((result['sl'][result['sl'].notnull()] == result.index[result['sl'].notnull()]).any())

            self.assertTrue(result['vb'].isnull().any())
            self.assertTrue(result['vb'].notnull().any())
            self.assertTrue((result['vb'][result['vb'].notnull()] > result.index[result['vb'].notnull()]).all())

            df = provider.request_data(BarsFilter(ticker=["IBM", "AAPL"], interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df['pt'] = 0.001
            df['sl'] = 0.001

            result_np = triple_barriers(df['close'], df['pt'], sl=df['sl'], vb=pd.Timedelta('36000s'), parallel=False).sort_index()
            self.assertTrue(isinstance(result_np.index, pd.MultiIndex))

            tmp = result_np.reset_index()['timestamp']
            pt_tmp = result_np['pt'].reset_index(drop=True)
            self.assertTrue(pt_tmp.isnull().any())
            self.assertFalse((pt_tmp[pt_tmp.notnull()] == tmp[pt_tmp.notnull()]).any())

            sl_tmp = result_np['sl'].reset_index(drop=True)
            self.assertTrue(sl_tmp.isnull().any())
            self.assertFalse((sl_tmp[sl_tmp.notnull()] == tmp[sl_tmp.notnull()]).any())

            result_p = triple_barriers(df['close'], df['pt'], sl=df['pt'], vb=pd.Timedelta('36000s'), parallel=True)
            assert_frame_equal(result_np, result_p.sort_index())

    def test_triple_barriers_performance(self):
        logging.basicConfig(level=logging.DEBUG)

        batch_len = 15000
        batch_width = 2000

        now = datetime.datetime.now()
        with IQFeedHistoryProvider() as provider:
            df1 = provider.request_data(BarsFilter(ticker="AAPL", interval_len=600, interval_type='s', max_bars=batch_len), sync_timestamps=False)

            df = {'AAPL': df1}
            for i in range(batch_width):
                df['AAPL_' + str(i)] = df1.sample(random.randint(int(len(df1) / 3), len(df1) - 1))

            df = pd.concat(df)
            df.index.set_names(['symbol', 'timestamp'], inplace=True)
            df.sort_index(inplace=True)
            df['pt'] = 0.001

            logging.getLogger(__name__).debug('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(df.shape))

            now = datetime.datetime.now()

            result = triple_barriers(df['close'], df['pt'], sl=df['pt'], vb=pd.Timedelta('6000s'), parallel=True)
            logging.getLogger(__name__).debug('Task done in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(result.shape))
