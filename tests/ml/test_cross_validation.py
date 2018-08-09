import unittest

from atpy.data.iqfeed.iqfeed_history_provider import *
from atpy.ml.cross_validation import cv_split, cv_split_reverse, cv_purge
from atpy.ml.util import *


class TestDataUtil(unittest.TestCase):

    def test_cv_split_1(self):
        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df['interval_end'] = df.index.shift(50, pd.Timedelta('3600s'))
            df.loc[df['interval_end'] > df.index[-1], 'interval_end'] = df.index[-1]
            df = df.dropna()

            # test 1
            split = cv_split(df, 5, 2, 0.02)

            self.assertTrue(split.size > 0)

            df['include'] = False
            df.loc[split, 'include'] = True
            incl = df['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertFalse(incl[0])
            self.assertFalse(incl[-1])

            purge = cv_purge(df['interval_end'], 5, 2, 0.02)
            if purge.size > 0:
                self.assertFalse(df['include'].loc[purge].any())

            # test 2
            split = cv_split(df, 5, 0, 0.02)

            self.assertTrue(split.size > 0)

            df['include'] = False
            df.loc[split, 'include'] = True
            incl = df['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertTrue(incl[0])
            self.assertFalse(incl[-1])

            purge = cv_purge(df['interval_end'], 5, 0, 0.02)
            if purge.size > 0:
                self.assertFalse(df['include'].loc[purge].any())

            # test 3
            split = cv_split(df, 5, 4, 0.02)

            self.assertTrue(split.size > 0)

            df['include'] = False
            df.loc[split, 'include'] = True
            incl = df['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertFalse(incl[0])
            self.assertTrue(incl[-1])

            purge = cv_purge(df['interval_end'], 5, 4, 0.02)
            if purge.size > 0:
                self.assertFalse(df['include'].loc[purge].any())

    def test_cv_split_2(self):
        with IQFeedHistoryProvider() as provider:
            df_1 = provider.request_data(BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df_1['interval_end'] = df_1.index.shift(50, pd.Timedelta('3600s'))
            df_1.loc[df_1['interval_end'] > df_1.index[-1], 'interval_end'] = df_1.index[-1]

            df_2 = provider.request_data(BarsFilter(ticker="IBM", interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df_2['interval_end'] = df_2.index.shift(50, pd.Timedelta('3600s'))
            df_2.loc[df_2['interval_end'] > df_2.index[-1], 'interval_end'] = df_2.index[-1]

            df = pd.concat({'AAPL': df_1, 'IBM': df_2})
            df.index.set_names('symbol', level=0, inplace=True)

            # test 1
            split = cv_split(df, 5, 2, 0.02)

            self.assertTrue(split.size > 0)

            df_tmp = df.reset_index(level=df.index.names.index('symbol'), drop=True).sort_index()

            df_tmp['include'] = False
            df_tmp.loc[split, 'include'] = True
            incl = df_tmp['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertFalse(incl[0])
            self.assertFalse(incl[-1])

            # test 2
            split = cv_split(df, 5, 0, 0.02)

            self.assertTrue(split.size > 0)

            df_tmp = df.reset_index(level=df.index.names.index('symbol'), drop=True).sort_index()

            df_tmp['include'] = False
            df_tmp.loc[split, 'include'] = True
            incl = df_tmp['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertTrue(incl[0])
            self.assertFalse(incl[-1])

            # test 3
            split = cv_split(df, 5, 4, 0.02)

            self.assertTrue(split.size > 0)

            df_tmp = df.reset_index(level=df.index.names.index('symbol'), drop=True).sort_index()

            df_tmp['include'] = False
            df_tmp.loc[split, 'include'] = True
            incl = df_tmp['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertFalse(incl[0])
            self.assertTrue(incl[-1])

    def test_cv_split_reverse_1(self):
        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df['interval_end'] = df.index.shift(50, pd.Timedelta('3600s'))
            df.loc[df['interval_end'] > df.index[-1], 'interval_end'] = df.index[-1]
            df = df.dropna()

            # test 1
            split = cv_split_reverse(df, 5, 2, 0.02)
            self.assertTrue(split.size > 0)

            df['include'] = False
            df.loc[split, 'include'] = True
            incl = df['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertTrue(incl[0])
            self.assertTrue(incl[-1])

            purge = cv_purge(df['interval_end'], 5, 2, 0.02)
            if purge.size > 0:
                self.assertFalse(df['include'].loc[purge].any())

            # test 2
            split = cv_split_reverse(df, 5, 0, 0.02)
            self.assertTrue(split.size > 0)

            df['include'] = False
            df.loc[split, 'include'] = True
            incl = df['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertFalse(incl[0])
            self.assertTrue(incl[-1])

            purge = cv_purge(df['interval_end'], 5, 0, 0.02)
            if purge.size > 0:
                self.assertFalse(df['include'].loc[purge].any())

            # test 3
            split = cv_split_reverse(df, 5, 4, 0.02)
            self.assertTrue(split.size > 0)

            df['include'] = False
            df.loc[split, 'include'] = True
            incl = df['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertTrue(incl[0])
            self.assertFalse(incl[-1])

            purge = cv_purge(df['interval_end'], 5, 4, 0.02)
            if purge.size > 0:
                self.assertFalse(df['include'].loc[purge].any())

    def test_cv_split_reverse_2(self):
        with IQFeedHistoryProvider() as provider:
            df_1 = provider.request_data(BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df_1['interval_end'] = df_1.index.shift(50, pd.Timedelta('3600s'))
            df_1.loc[df_1['interval_end'] > df_1.index[-1], 'interval_end'] = df_1.index[-1]

            df_2 = provider.request_data(BarsFilter(ticker="IBM", interval_len=3600, interval_type='s', max_bars=1000), sync_timestamps=False)
            df_2['interval_end'] = df_2.index.shift(50, pd.Timedelta('3600s'))
            df_2.loc[df_2['interval_end'] > df_2.index[-1], 'interval_end'] = df_2.index[-1]

            df = pd.concat({'AAPL': df_1, 'IBM': df_2})
            df.index.set_names('symbol', level=0, inplace=True)

            # test 1
            split = cv_split_reverse(df, 5, 2, 0.02)

            self.assertTrue(split.size > 0)

            df_tmp = df.reset_index(level=df.index.names.index('symbol'), drop=True).sort_index()

            df_tmp['include'] = False
            df_tmp.loc[split, 'include'] = True
            incl = df_tmp['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertTrue(incl[0])
            self.assertTrue(incl[-1])

            # test 2
            split = cv_split_reverse(df, 5, 0, 0.02)

            self.assertTrue(split.size > 0)

            df_tmp = df.reset_index(level=df.index.names.index('symbol'), drop=True).sort_index()

            df_tmp['include'] = False
            df_tmp.loc[split, 'include'] = True
            incl = df_tmp['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertFalse(incl[0])
            self.assertTrue(incl[-1])

            # test 3
            split = cv_split_reverse(df, 5, 4, 0.02)

            self.assertTrue(split.size > 0)

            df_tmp = df.reset_index(level=df.index.names.index('symbol'), drop=True).sort_index()

            df_tmp['include'] = False
            df_tmp.loc[split, 'include'] = True
            incl = df_tmp['include']
            self.assertTrue(incl[incl].size > 0)
            self.assertTrue(incl[~incl].size > 0)
            self.assertTrue(incl[0])
            self.assertFalse(incl[-1])
