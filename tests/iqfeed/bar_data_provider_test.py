import unittest
from atpy.data.iqfeed.iqfeed_bar_data_provider import *
import queue


class TestIQFeedBarData(unittest.TestCase):
    """
    IQFeed bar data test, which checks whether the class works in basic terms
    """

    def test_provider_column_mode(self):
        with IQFeedBarDataProvider(minibatch=2, column_mode=True) as provider:
            provider.process_bar += lambda *args, **kwargs: self.assertEqual(kwargs[FUNCTION_OUTPUT]['symbol'], 'SPY'.encode(encoding='UTF-8'))
            provider.process_bar_batch += lambda *args, **kwargs: self.assertEqual(kwargs[FUNCTION_OUTPUT]['symbol'][0], 'SPY'.encode(encoding='UTF-8'))

            provider.watch(symbol='SPY', interval_len=5,
                           interval_type='s', update=1, lookback_bars=10)

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 10)
                self.assertEqual(len(d['symbol']), 2)
                self.assertEqual(d['symbol'][0], 'SPY'.encode(encoding='UTF-8'))

                if i == 1:
                    break

    def test_provider_row_mode(self):
        with IQFeedBarDataProvider(minibatch=2, column_mode=False) as provider:
            provider.process_bar += lambda *args, **kwargs: self.assertEqual(kwargs[FUNCTION_OUTPUT]['symbol'], 'SPY'.encode(encoding='UTF-8'))
            provider.process_bar_batch += lambda *args, **kwargs: self.assertEqual(kwargs[FUNCTION_OUTPUT][0]['symbol'], 'SPY'.encode(encoding='UTF-8'))

            provider.watch(symbol='SPY', interval_len=5,
                           interval_type='s', update=1, lookback_bars=10)

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 2)
                self.assertEqual(len(d[0]), 10)
                self.assertEqual(d[0]['symbol'], 'SPY'.encode(encoding='UTF-8'))

                if i == 1:
                    break

    def test_listener(self):
        with IQFeedBarDataListener(minibatch=2, column_mode=True) as provider:

            q = queue.Queue()
            provider.process_bar += lambda *args, **kwargs: self.assertEqual(kwargs[FUNCTION_OUTPUT]['symbol'], 'SPY'.encode(encoding='UTF-8'))
            provider.process_bar_batch += lambda *args, **kwargs: q.put(kwargs[FUNCTION_OUTPUT].data)

            provider.watch(symbol='SPY', interval_len=5,
                           interval_type='s', update=1, lookback_bars=10)

            for i, d in enumerate(iter(q.get, None)):
                self.assertEqual(len(d), 10)
                self.assertEqual(len(d['symbol']), 2)
                self.assertEqual(d['symbol'][0], 'SPY'.encode(encoding='UTF-8'))

                if i == 1:
                    break

if __name__ == '__main__':
    unittest.main()
