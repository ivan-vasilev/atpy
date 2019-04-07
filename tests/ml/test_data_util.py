import random
import unittest

from pandas.util.testing import assert_frame_equal

from atpy.data.iqfeed.iqfeed_history_provider import *
from atpy.ml.frac_diff_features import get_weights_ffd, frac_diff_ffd
from atpy.ml.util import *


class TestDataUtil(unittest.TestCase):

    def test_daily_volatility_1(self):
        batch_len = 100

        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=batch_len))
            result = daily_volatility(df['close'], parallel=False)
            self.assertTrue(isinstance(result.index, pd.DatetimeIndex))
            self.assertGreater(result.size, 0)

            df = provider.request_data(BarsFilter(ticker=["AAPL", "IBM"], interval_len=3600, interval_type='s', max_bars=batch_len), sync_timestamps=False)
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

    def test_cumsum_filter_1(self):
        batch_len = 100

        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len))
            df = df['close'].to_frame()
            df['threshold'] = 0.01

            result = cumsum_filter(df['close'], df['threshold'])
            self.assertTrue(isinstance(result, pd.DatetimeIndex))
            self.assertGreater(result.size, 0)

            df = provider.request_data(BarsFilter(ticker=["AAPL", "IBM"], interval_len=60, interval_type='s', max_bars=batch_len))
            df = df['close'].to_frame()
            df['threshold'] = 0.01

            result_np = cumsum_filter(df['close'], df['threshold'], parallel=False)
            self.assertTrue(isinstance(result_np, pd.MultiIndex))
            self.assertEqual(len(result_np.levels), 2)
            self.assertEqual(result_np.names, ['symbol', 'timestamp'])
            self.assertGreater(result_np.size, 0)

            result_p = cumsum_filter(df['close'], df['threshold'], parallel=True)
            assert_frame_equal(pd.DataFrame(index=result_np), pd.DataFrame(index=result_p))

            df = df.reorder_levels(['timestamp', 'symbol']).sort_index()
            result_np = cumsum_filter(df['close'], df['threshold'], parallel=False)

            self.assertTrue(isinstance(result_np, pd.MultiIndex))
            self.assertEqual(len(result_np.levels), 2)
            self.assertEqual(result_np.names, ['timestamp', 'symbol'])
            self.assertGreater(result_np.size, 0)

            result_p = cumsum_filter(df['close'], df['threshold'], parallel=True)
            assert_frame_equal(pd.DataFrame(index=result_np), pd.DataFrame(index=result_p))

    def test_cumsum_filter_performance(self):
        logging.basicConfig(level=logging.DEBUG)

        batch_len = 15000
        batch_width = 2000

        with IQFeedHistoryProvider() as provider:
            now = datetime.datetime.now()

            df1 = provider.request_data(BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len), sync_timestamps=False)

            dfs1 = {'AAPL': df1}
            for i in range(batch_width):
                dfs1['AAPL_' + str(i)] = df1.sample(random.randint(int(len(df1) / 3), len(df1) - 1))

            dfs1 = pd.concat(dfs1).swaplevel(0, 1)
            dfs1.index.set_names(['timestamp', 'symbol'], inplace=True)
            dfs1.sort_index(inplace=True)

            logging.getLogger(__name__).debug('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(dfs1.shape))

            now = datetime.datetime.now()
            close = dfs1['close'].to_frame()
            close['threshold'] = 0.01

            x = cumsum_filter(close['close'], close['threshold'], parallel=True)

            elapsed = datetime.datetime.now() - now
            logging.getLogger(__name__).debug('Result shape: ' + str(x.shape))
            logging.getLogger(__name__).debug('Time elapsed ' + str(elapsed) + ' for ' + str(i + 1) + ' iterations; ' + str(elapsed / (i % 1000)) + ' per iteration')

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

    def test_merge_bars_to_last_correctness(self):
        open_p = np.array([10, 20, 30, 40, 50, 60, 70, 80, 90], dtype=np.float32)
        high_p = np.array([1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000], dtype=np.float32)
        low_p = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9], dtype=np.float32)
        close_p = np.array([100, 200, 300, 400, 500, 600, 700, 800, 900], dtype=np.float32)
        volume = np.array([2, 2, 5, 1, 2, 7, 7, 1, 1], dtype=np.float32)

        df = pd.DataFrame.from_dict({'open': open_p, 'high': high_p, 'low': low_p, 'close': close_p, 'volume': volume})
        merged_1 = merge_bars_to_last(df.copy(deep=True), threshold=3).reset_index(drop=True)
        self.assertEqual(9, df.shape[0])
        self.assertEqual(6, merged_1.shape[0])
        self.assertEqual(merged_1['volume'].sum(), df['volume'].sum())
        np.testing.assert_array_equal(merged_1['volume'].values, np.array([4, 5, 3, 7, 7, 2], dtype=np.float32))
        np.testing.assert_array_equal(merged_1['low'].values, np.array([1, 3, 4, 6, 7, 8], dtype=np.float32))

        df_copy = df.copy(deep=True)
        df_copy['symbol'] = 'SYMBOL_2'

        multiind_df = pd.concat({'SYMBOL_1': df, 'SYMBOL_2': df.copy(deep=True)})
        multiind_df.index.set_names('symbol', level=0, inplace=True)
        multiind_df.sort_index(inplace=True)

        merged_single_thread = merge_bars_to_last(multiind_df.copy(deep=True), threshold=3, parallel=False)
        merged_parallel = merge_bars_to_last(multiind_df.copy(deep=True), threshold=3, parallel=True)

        assert_frame_equal(merged_single_thread, merged_parallel)

        slice_1 = merged_single_thread.loc[pd.IndexSlice['SYMBOL_1', :]].reset_index(drop=True)
        slice_2 = merged_single_thread.loc[pd.IndexSlice['SYMBOL_2', :]].reset_index(drop=True)
        assert_frame_equal(slice_1, slice_2)
        assert_frame_equal(slice_1, merged_1)

    def test_merge_bars_to_last_real_data(self):
        batch_len = 1000

        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker=["AAPL", "IBM"], interval_len=60, interval_type='s', max_bars=batch_len)) \
                .rename({'period_volume': 'volume'}, axis=1) \
                .drop(['symbol', 'timestamp', 'total_volume', 'number_of_trades'], axis=1)

            mean = df['volume'].mean()
            merged_1 = merge_bars_to_last(df, threshold=mean, parallel=False)
            self.assertLess(merged_1.shape[0], df.shape[0])
            self.assertEqual(merged_1['volume'].sum(), df['volume'].sum())

            for s in merged_1.index.get_level_values('symbol').unique():
                self.assertGreaterEqual(merged_1.loc[s]['volume'][:-1].min(), mean)

            merged_2 = merge_bars_to_last(df, threshold=mean, parallel=True)

            assert_frame_equal(merged_1, merged_2)

    def test_merge_bars_to_last_performance_single(self):
        logging.basicConfig(level=logging.INFO)
        batch = 10000
        depth = 300
        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=600, interval_type='s', max_bars=depth), sync_timestamps=False). \
                rename({'period_volume': 'volume'}, axis=1) \
                .drop(['symbol', 'timestamp', 'total_volume', 'number_of_trades'], axis=1)
            df.sort_index(inplace=True)

            logging.getLogger(__name__).info('DataFrame shape ' + str(df.shape))

            data = [df.copy(deep=True) for _ in range(batch)]
            mean = df['volume'].mean()
            merge_bars_to_last(df.copy(deep=True), threshold=mean, parallel=False)

            now = datetime.datetime.now()
            for d in data:
                result = merge_bars_to_last(d, threshold=mean, parallel=False)
            delta = datetime.datetime.now() - now

            logging.getLogger(__name__).info('Result shape ' + str(result.shape))
            logging.getLogger(__name__).info('Task done in ' + str(delta) + '; ' + str(delta / batch) + ' per iteration')

            self.assertLess(result.shape[0], df.shape[0])
            self.assertEqual(result['volume'].sum(), df['volume'].sum())

    def test_merge_bars_to_last_performance_wide(self):
        df = self._generate_random_merge_bars_data(batch_len=200, batch_width=10000)
        self._test_merge_bars_to_last_performance(df, df['volume'].mean(), parallel=False)

    def test_merge_bars_to_last_performance_deep(self):
        df = self._generate_random_merge_bars_data(batch_len=15000, batch_width=2000)
        self._test_merge_bars_to_last_performance(df, df['volume'].mean(), parallel=False)

    @staticmethod
    def _generate_random_merge_bars_data(batch_len, batch_width):
        logging.basicConfig(level=logging.INFO)

        now = datetime.datetime.now()
        with IQFeedHistoryProvider() as provider:
            df1 = provider.request_data(BarsFilter(ticker="AAPL", interval_len=600, interval_type='s', max_bars=batch_len), sync_timestamps=False). \
                rename({'period_volume': 'volume'}, axis=1) \
                .drop(['symbol', 'timestamp', 'total_volume', 'number_of_trades'], axis=1)

            df = {'AAPL': df1}
            for i in range(batch_width):
                df['AAPL_' + str(i)] = df1.sample(random.randint(int(len(df1) / 3), len(df1) - 1))

            df = pd.concat(df)
            df.index.set_names(['symbol', 'timestamp'], inplace=True)
            df.sort_index(inplace=True)

            logging.getLogger(__name__).info('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(df.shape))

        return df

    def _test_merge_bars_to_last_performance(self, df, mean, parallel=True):
        now = datetime.datetime.now()

        result = merge_bars_to_last(df, threshold=mean, parallel=parallel)
        logging.getLogger(__name__).info('Task done in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(result.shape))

        self.assertLess(result.shape[0], df.shape[0])
        self.assertEqual(result['volume'].sum(), df['volume'].sum())

        for s in result.index.get_level_values('symbol').unique():
            self.assertGreaterEqual(result.loc[s]['volume'][:-1].min(), mean)


if __name__ == '__main__':
    unittest.main()
