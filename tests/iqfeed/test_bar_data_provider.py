import unittest

from atpy.data.iqfeed.iqfeed_bar_data_provider import *


class TestIQFeedBarData(unittest.TestCase):
    """
    IQFeed bar data test, which checks whether the class works in basic terms
    """

    def test_provider_column_mode(self):
        with IQFeedBarDataProvider(minibatch=2, column_mode=True) as provider:
            e1 = threading.Event()

            provider.process_bar += lambda event: [self.assertEqual(event['symbol'], 'SPY'.encode(encoding='UTF-8')), e1.set()]

            e2 = threading.Event()

            provider.process_bar_batch += lambda event: [self.assertEqual(event['symbol'][0], 'SPY'.encode(encoding='UTF-8')), e2.set()]

            provider.watch(symbol='SPY', interval_len=5,
                           interval_type='s', update=1, lookback_bars=10)

            e1.wait()
            e2.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 10)
                self.assertEqual(len(d['symbol']), 2)
                self.assertEqual(d['symbol'][0], 'SPY'.encode(encoding='UTF-8'))

                if i == 1:
                    break

    def test_provider_row_mode(self):
        with IQFeedBarDataProvider(minibatch=2, column_mode=False) as provider:
            e1 = threading.Event()
            provider.process_bar += lambda event: [self.assertEqual(event['symbol'], 'SPY'.encode(encoding='UTF-8')), e1.set()]

            e2 = threading.Event()
            provider.process_bar_batch += lambda event: [self.assertEqual(event[0]['symbol'], 'SPY'.encode(encoding='UTF-8')), e2.set()]

            provider.watch(symbol='SPY', interval_len=5,
                           interval_type='s', update=1, lookback_bars=10)

            e1.wait()
            e2.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 2)
                self.assertEqual(len(d[0]), 10)
                self.assertEqual(d[0]['symbol'], 'SPY'.encode(encoding='UTF-8'))

                if i == 1:
                    break

    def test_listener(self):
        with IQFeedBarDataListener(minibatch=2, column_mode=True) as provider:
            q = queue.Queue()

            e1 = threading.Event()

            provider.process_bar += lambda event: [self.assertEqual(event['symbol'], 'SPY'.encode(encoding='UTF-8')), e1.set()]

            provider.process_bar_batch += lambda event: q.put(event.data)

            provider.watch(symbol='SPY', interval_len=5,
                           interval_type='s', update=1, lookback_bars=10)

            e1.wait()

            for i, d in enumerate(iter(q.get, None)):
                self.assertEqual(len(d), 10)
                self.assertEqual(len(d['symbol']), 2)
                self.assertEqual(d['symbol'][0], 'SPY'.encode(encoding='UTF-8'))

                if i == 1:
                    break

if __name__ == '__main__':
    unittest.main()
