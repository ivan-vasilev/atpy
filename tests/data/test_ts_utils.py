import unittest

import atpy.data.tradingcalendar as tcal
from atpy.data.iqfeed.iqfeed_history_provider import *
from atpy.data.ts_util import set_periods, current_period


class TestTSUtils(unittest.TestCase):

    def test_set_periods(self):
        batch_len = 1000

        with IQFeedHistoryProvider() as provider:
            # One symbol, all periods
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=300, interval_type='s', max_bars=batch_len), sync_timestamps=False, adjust_data=False)

            set_periods(df)
            self.assertTrue('sequence' in df.columns and 'period' in df.columns)
            self.assertGreater(len(pd.unique(df['sequence'].dropna())), 0)
            self.assertEqual(len(pd.unique(df['period'].dropna())), 2)
            self.assertEqual(len(df['period'].dropna()), len(df['period']))
            self.assertEqual(len(df['sequence'].dropna()), len(df['sequence']))

            # Multiple symbols, all periods
            df = provider.request_data(BarsFilter(ticker=["AAPL", "IBM"], interval_len=300, interval_type='s', max_bars=batch_len), sync_timestamps=False, adjust_data=False).swaplevel(0, 1).sort_index()

            set_periods(df)
            self.assertTrue('sequence' in df.columns and 'period' in df.columns)
            self.assertGreater(len(pd.unique(df['sequence'].dropna())), 0)
            self.assertEqual(len(pd.unique(df['period'].dropna())), 2)
            self.assertEqual(len(df['period'].dropna()), len(df['period']))
            self.assertEqual(len(df['sequence'].dropna()), len(df['sequence']))

            # Multiple symbols, N periods
            df = provider.request_data(BarsFilter(ticker=["AAPL", "IBM"], interval_len=300, interval_type='s', max_bars=batch_len), sync_timestamps=False, adjust_data=False).swaplevel(0, 1).sort_index()
            lc = tcal.open_and_closes.loc[min(df['timestamp']): max(df['timestamp'])].iloc[::-1]
            xs = pd.IndexSlice
            df = df.loc[xs[:lc.iloc[0]['market_close'], :]].iloc[:-3]
            set_periods(df)
            self.assertTrue('sequence' in df.columns and 'period' in df.columns)
            self.assertGreater(len(pd.unique(df['sequence'].dropna())), 0)
            self.assertEqual(len(pd.unique(df['period'].dropna())), 2)
            self.assertEqual(len(df['period'].dropna()), len(df['period']))
            self.assertEqual(len(df['sequence'].dropna()), len(df['sequence']))

    def test_current_period(self):
        batch_len = 1000

        with IQFeedHistoryProvider() as provider:
            # One symbol, all periods
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=300, interval_type='s', max_bars=batch_len), sync_timestamps=False, adjust_data=False)

            slc = current_period(df)
            self.assertTrue('sequence' in slc.columns and 'period' in slc.columns)
            self.assertEqual(len(pd.unique(slc['sequence'].dropna())), 1)
            self.assertEqual(len(pd.unique(slc['period'].dropna())), 1)
            self.assertEqual(len(slc['period'].dropna()), len(slc['period']))
            self.assertEqual(len(slc['sequence'].dropna()), len(slc['sequence']))
            self.assertGreater(len(df), len(slc))

            # Multiple symbols, all periods
            df = provider.request_data(BarsFilter(ticker=["AAPL", "IBM"], interval_len=300, interval_type='s', max_bars=batch_len), sync_timestamps=False, adjust_data=False).swaplevel(0, 1).sort_index()

            slc = current_period(df)
            self.assertTrue('sequence' in slc.columns and 'period' in slc.columns)
            self.assertEqual(len(pd.unique(slc['sequence'].dropna())), 1)
            self.assertEqual(len(pd.unique(slc['period'].dropna())), 1)
            self.assertEqual(len(slc['period'].dropna()), len(slc['period']))
            self.assertEqual(len(slc['sequence'].dropna()), len(slc['sequence']))
            self.assertGreater(len(df), len(slc))

            df = provider.request_data(BarsFilter(ticker=["AAPL", "IBM"], interval_len=300, interval_type='s', max_bars=batch_len), sync_timestamps=False, adjust_data=False).swaplevel(0, 1).sort_index()
            lc = tcal.open_and_closes.loc[min(df['timestamp']): max(df['timestamp'])].iloc[::-1]
            xs = pd.IndexSlice
            df = df.loc[xs[:lc.iloc[0]['market_close'], :]].iloc[:-3]
            slc = current_period(df)
            self.assertTrue('sequence' in slc.columns and 'period' in slc.columns)
            self.assertEqual(len(pd.unique(slc['sequence'].dropna())), 1)
            self.assertEqual(len(pd.unique(slc['period'].dropna())), 1)
            self.assertEqual(len(slc['period'].dropna()), len(slc['period']))
            self.assertEqual(len(slc['sequence'].dropna()), len(slc['sequence']))
            self.assertGreater(len(df), len(slc))
