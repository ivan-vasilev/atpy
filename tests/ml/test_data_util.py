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

    def test_triple_barriers_side(self):
        with IQFeedHistoryProvider() as provider:
            # test single index
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df['pt'] = 0.001
            df['sl'] = 0.001

            result = triple_barriers(df['close'], df['pt'], sl=df['sl'], vb=pd.Timedelta('36000s'), parallel=False)
            self.assertTrue(result['barrier_hit'].isnull().any())
            self.assertTrue(result['barrier_hit'].notnull().any())
            self.assertFalse((result['barrier_hit'][result['barrier_hit'].notnull()] == result.index[result['barrier_hit'].notnull()]).any())

            self.assertTrue((result['side'] == 1).any())
            self.assertTrue((result['side'] == -1).any())
            self.assertTrue((result['side'] == 0).any())

            self.assertTrue(result['returns'].isnull().any())
            self.assertTrue(result['returns'].notnull().any())

            # test seed values
            df['seed'] = False
            df.iloc[:10]['seed'] = True
            result_seed = triple_barriers(df['close'], df['pt'], sl=df['sl'], vb=pd.Timedelta('36000s'), seed=df['seed'], parallel=False).dropna()
            self.assertLessEqual(result_seed.shape[0], 10)

            # test multiindex
            df = provider.request_data(BarsFilter(ticker=["IBM", "AAPL"], interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df['pt'] = 0.001
            df['sl'] = 0.001

            result_np = triple_barriers(df['close'], df['pt'], sl=df['sl'], vb=pd.Timedelta('36000s'), parallel=False).sort_index()
            self.assertTrue(isinstance(result_np.index, pd.MultiIndex))

            tmp = result_np.reset_index()['timestamp']
            barrier_hit_tmp = result_np['barrier_hit'].reset_index(drop=True)
            self.assertTrue(barrier_hit_tmp.isnull().any())
            self.assertTrue(barrier_hit_tmp.notnull().any())
            self.assertFalse((barrier_hit_tmp[barrier_hit_tmp.notnull()] == tmp[barrier_hit_tmp.notnull()]).any())

            result_p = triple_barriers(df['close'], pt=df['pt'], sl=df['pt'], vb=pd.Timedelta('36000s'), parallel=True)
            assert_frame_equal(result_np, result_p.sort_index())

            # test seed values
            df['seed'] = False

            def tmp(x):
                x.iloc[:10]['seed'] = True
                return x

            df = df.groupby(level='symbol').apply(tmp)
            result_seed = triple_barriers(df['close'], df['pt'], sl=df['sl'], vb=pd.Timedelta('36000s'), seed=df['seed'], parallel=False).dropna()
            self.assertLessEqual(result_seed.shape[0], 20)

    def test_triple_barriers_size(self):
        with IQFeedHistoryProvider() as provider:
            # test single symbol
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df['pt'] = 0.001
            df['sl'] = 0.001
            df['side'] = 1

            result = triple_barriers(df['close'], df['pt'], sl=df['sl'], side=df['side'], vb=pd.Timedelta('36000s'), parallel=False)
            self.assertTrue(result['barrier_hit'].isnull().any())
            self.assertTrue(result['barrier_hit'].notnull().any())
            self.assertFalse((result['barrier_hit'][result['barrier_hit'].notnull()] == result.index[result['barrier_hit'].notnull()]).any())

            self.assertTrue(result['returns'].isnull().any())
            self.assertTrue(result['returns'].notnull().any())

            self.assertTrue((result['size'] == 1).any())
            self.assertTrue((result['size'] == 0).any())
            self.assertTrue(result.loc[result['returns'] <= 0.001, 'size'].max() == 0)
            self.assertTrue(result.loc[result['returns'] > 0.001, 'size'].min() == 1)

            df['side'] = -1

            result = triple_barriers(df['close'], df['pt'], sl=df['sl'], side=df['side'], vb=pd.Timedelta('36000s'), parallel=False)

            self.assertTrue((result['size'] == 1).any())
            self.assertTrue((result['size'] == 0).any())
            self.assertTrue(result.loc[result['returns'] > 0.001, 'size'].max() == 0)
            self.assertTrue(result.loc[result['returns'] <= -0.001, 'size'].min() == 1)

            # test seed values
            df['seed'] = False
            df.iloc[:10]['seed'] = True
            result_seed = triple_barriers(df['close'], df['pt'], sl=df['sl'], side=df['side'], vb=pd.Timedelta('36000s'), seed=df['seed'], parallel=False).dropna()
            self.assertLessEqual(result_seed.shape[0], 10)

            # test multiindex
            df = provider.request_data(BarsFilter(ticker=["IBM", "AAPL"], interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df['pt'] = 0.001
            df['sl'] = 0.001
            df['side'] = 1

            result_np = triple_barriers(df['close'], df['pt'], sl=df['sl'], side=df['side'], vb=pd.Timedelta('36000s'), parallel=True).sort_index()
            self.assertTrue(isinstance(result_np.index, pd.MultiIndex))

            tmp = result_np.reset_index()['timestamp']
            barrier_hit_tmp = result_np['barrier_hit'].reset_index(drop=True)
            self.assertTrue(barrier_hit_tmp.isnull().any())
            self.assertTrue(barrier_hit_tmp.notnull().any())
            self.assertFalse((barrier_hit_tmp[barrier_hit_tmp.notnull()] == tmp[barrier_hit_tmp.notnull()]).any())

            self.assertTrue((result_np['size'] == 1).any())
            self.assertTrue((result_np['size'] == 0).any())

            result_p = triple_barriers(df['close'], df['pt'], sl=df['pt'], side=df['side'], vb=pd.Timedelta('36000s'), parallel=True)
            assert_frame_equal(result_np, result_p.sort_index())

            # test seed values
            df['seed'] = False

            def tmp(x):
                x.iloc[:10]['seed'] = True
                return x

            df = df.groupby(level='symbol').apply(tmp)
            result_seed = triple_barriers(df['close'], df['pt'], sl=df['sl'], side=df['side'], vb=pd.Timedelta('36000s'), seed=df['seed'], parallel=False).dropna()
            self.assertLessEqual(result_seed.shape[0], 20)

    def test_triple_barriers_side_performance(self):
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
            df['sl'] = 0.001

            logging.getLogger(__name__).debug('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(df.shape))

            now = datetime.datetime.now()

            result = triple_barriers(df['close'], pt=df['pt'], sl=df['sl'], vb=pd.Timedelta('6000s'), parallel=True)
            logging.getLogger(__name__).debug('Task done in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(result.shape))

    def test_triple_barriers_size_performance(self):
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
            df['sl'] = 0.001
            df['side'] = 1

            logging.getLogger(__name__).debug('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(df.shape))

            now = datetime.datetime.now()

            result = triple_barriers(df['close'], pt=df['pt'], sl=df['pt'], side=df['side'], vb=pd.Timedelta('6000s'), parallel=True)
            logging.getLogger(__name__).debug('Task done in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(result.shape))

            self.assertTrue((result['size'] == 1).any())
            self.assertTrue((result['size'] == 0).any())

    def test_frac_diff_ffd(self):
        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=100), sync_timestamps=False)
            series = df['close']

            result = frac_diff_ffd(series, 0.4, threshold=1e-2)

            w = get_weights_ffd(d=0.4, threshold=1e-2)
            width = len(w) - 1

            result_2 = pd.Series()
            for iloc1 in range(width, series.shape[0]):
                loc0, loc1 = series.index[iloc1 - width], series.index[iloc1]
                if not np.isfinite(series.loc[loc1]):
                    continue  # exclude NAs
                result_2[loc1] = np.dot(w, series.loc[loc0:loc1])

            self.assertTrue((result.values == result_2.values).all())
            self.assertTrue((result.index == result_2.index).all())

            # multiindex
            series = provider.request_data(BarsFilter(ticker=["AAPL", "GOOG"], interval_len=3600, interval_type='s', max_bars=100), sync_timestamps=False)['close']
            result = frac_diff_ffd(series, 0.4, threshold=1e-2, parallel=False)

            self.assertTrue(isinstance(result.index, pd.MultiIndex))
            self.assertFalse(result.empty)

            result_p = frac_diff_ffd(series, 0.4, threshold=1e-2, parallel=True)

            assert_frame_equal(result.to_frame(), result_p.to_frame())

    def test_frac_diff_ffd_performance(self):
        logging.basicConfig(level=logging.DEBUG)

        batch_len = 15000
        batch_width = 2000

        now = datetime.datetime.now()
        with IQFeedHistoryProvider() as provider:
            df1 = provider.request_data(BarsFilter(ticker="AAPL", interval_len=600, interval_type='s', max_bars=batch_len), sync_timestamps=False)['close']

            df = {'AAPL': df1}
            for i in range(batch_width):
                df['AAPL_' + str(i)] = df1.sample(random.randint(int(len(df1) / 3), len(df1) - 1))

            df = pd.concat(df)
            df.index.set_names(['symbol', 'timestamp'], inplace=True)
            df.sort_index(inplace=True)

            logging.getLogger(__name__).debug('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(df.shape))

            now = datetime.datetime.now()

            result = frac_diff_ffd(df, 0.4, threshold=1e-2, parallel=True)
            logging.getLogger(__name__).debug('Task done in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(result.shape))

            self.assertTrue(isinstance(result.index, pd.MultiIndex))
            self.assertFalse(result.empty)
