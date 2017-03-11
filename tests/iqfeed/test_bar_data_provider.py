import unittest

from atpy.data.iqfeed.iqfeed_bar_data_provider import *


class TestIQFeedBarData(unittest.TestCase):
    """
    IQFeed bar data test, which checks whether the class works in basic terms
    """

    def test_provider_column_mode(self):
        with IQFeedBarDataListener(minibatch=2, column_mode=True) as listener, listener.bar_batch_provider() as provider:
            e1 = threading.Event()

            listener.on_bar += lambda event: [self.assertEqual(event['data']['Symbol'], 'SPY'), e1.set()]

            e2 = threading.Event()

            listener.on_bar_batch += lambda event: [self.assertEqual(event['data']['Symbol'][0], 'SPY'), e2.set()]

            listener.watch(symbol='SPY', interval_len=5,
                           interval_type='s', update=1, lookback_bars=10)

            e1.wait()
            e2.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 10)
                self.assertEqual(len(d['Symbol']), 2)
                self.assertEqual(d['Symbol'][0], 'SPY')

                if i == 1:
                    break

    def test_provider_row_mode(self):
        with IQFeedBarDataListener(minibatch=2, column_mode=False) as listener, listener.bar_batch_provider() as provider:
            e1 = threading.Event()
            listener.on_bar += lambda event: [self.assertEqual(event['data']['Symbol'], 'SPY'), e1.set()]

            e2 = threading.Event()
            listener.on_bar_batch += lambda event: [self.assertEqual(event['data'][0]['Symbol'], 'SPY'), e2.set()]

            listener.watch(symbol='SPY', interval_len=5,
                           interval_type='s', update=1, lookback_bars=10)

            e1.wait()
            e2.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 2)
                self.assertEqual(len(d[0]), 10)
                self.assertEqual(d[0]['Symbol'], 'SPY')

                if i == 1:
                    break

    def test_listener(self):
        with IQFeedBarDataListener(minibatch=2, column_mode=True) as listener:
            q = queue.Queue()

            e1 = threading.Event()

            listener.on_bar += lambda event: [self.assertEqual(event['data']['Symbol'], 'SPY'), e1.set()]

            listener.on_bar_batch += lambda event: q.put(event['data'])

            listener.watch(symbol='SPY', interval_len=5,
                           interval_type='s', update=1, lookback_bars=10)

            e1.wait()

            for i, d in enumerate(iter(q.get, None)):
                self.assertEqual(len(d), 10)
                self.assertEqual(len(d['Symbol']), 2)
                self.assertEqual(d['Symbol'][0], 'SPY')

                if i == 1:
                    break

if __name__ == '__main__':
    unittest.main()
