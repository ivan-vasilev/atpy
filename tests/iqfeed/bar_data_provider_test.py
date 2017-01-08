import unittest
from atpy.data.iqfeed.iqfeed_bar_data_provider import *


class TestIQFeedBarData(unittest.TestCase):
    """
    IQFeed bar data test, which checks whether the class works in basic terms
    """

    def test_provider(self):
        with IQFeedBarDataProvider(minibatch=2) as provider:
            provider.watch(symbol='SPY', interval_len=5,
                           interval_type='s', update=1, lookback_bars=10)

            for i, d in enumerate(provider):
                if i == 1:
                    break

            self.assertEqual(len(d), 10)
            self.assertEqual(len(d['symbol']), 2)
            self.assertEqual(d['symbol'][0], 'SPY'.encode(encoding='UTF-8'))

if __name__ == '__main__':
    unittest.main()
