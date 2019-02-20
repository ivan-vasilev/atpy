import unittest

import pandas as pd
import talib
from talib import abstract
from pandas.util.testing import assert_index_equal

from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsFilter


class TestTALib(unittest.TestCase):
    """Demonstrate how to use TA-lib"""

    def test_ta_lib_function_api(self):
        """Test the functional interface of TA-Lib"""

        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=300, interval_type='s', max_bars=1000), sync_timestamps=False)
            close = df['close']

            output = talib.SMA(close)
            self.assertTrue(isinstance(output, pd.Series))
            self.assertFalse(output.empty)
            self.assertTrue(pd.isna(output[0]))
            self.assertFalse(pd.isna(output[-1]))
            self.assertEqual(close.shape, output.shape)
            self.assertEqual(close.dtype, output.dtype)
            assert_index_equal(close.index, output.index)

            bbands = talib.BBANDS(close, matype=talib.MA_Type.T3)
            for bband in bbands:
                self.assertTrue(isinstance(bband, pd.Series))
                self.assertFalse(bband.empty)
                self.assertTrue(pd.isna(bband[0]))
                self.assertFalse(pd.isna(bband[-1]))
                self.assertEqual(close.shape, bband.shape)
                self.assertEqual(close.dtype, bband.dtype)
                assert_index_equal(close.index, bband.index)

    def test_ta_lib_abstract_api(self):
        """Test the abstract API of TA-Lib"""

        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker="AAPL", interval_len=300, interval_type='s', max_bars=1000), sync_timestamps=False)
            close = df['close']

            output = abstract.SMA(df)
            self.assertTrue(isinstance(output, pd.Series))
            self.assertFalse(output.empty)
            self.assertTrue(pd.isna(output[0]))
            self.assertFalse(pd.isna(output[-1]))
            self.assertEqual(close.shape, output.shape)
            self.assertEqual(close.dtype, output.dtype)
            assert_index_equal(close.index, output.index)

            bbands = abstract.BBANDS(df, matype=talib.MA_Type.T3)
            self.assertTrue(isinstance(bbands, pd.DataFrame))
            assert_index_equal(close.index, bbands.index)

            for _, bband in bbands.iteritems():
                self.assertTrue(isinstance(bband, pd.Series))
                self.assertFalse(bband.empty)
                self.assertEqual(close.shape, bband.shape)
                self.assertEqual(close.dtype, bband.dtype)
                self.assertTrue(pd.isna(bband[0]))
                self.assertFalse(pd.isna(bband[-1]))

            stoch = abstract.STOCH(df, 5, 3, 0, 3, 0)
            self.assertTrue(isinstance(stoch, pd.DataFrame))
            assert_index_equal(close.index, stoch.index)

            for _, s in stoch.iteritems():
                self.assertTrue(isinstance(s, pd.Series))
                self.assertFalse(s.empty)
                self.assertEqual(close.shape, s.shape)
                self.assertEqual(close.dtype, s.dtype)
                self.assertTrue(pd.isna(s[0]))
                self.assertFalse(pd.isna(s[-1]))
