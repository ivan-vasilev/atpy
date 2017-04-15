import unittest

from atpy.data.iqfeed.iqfeed_bar_data_provider import *


class TestIQFeedBarData(unittest.TestCase):
    """
    IQFeed bar data test, which checks whether the class works in basic terms
    """

    def test_provider(self):
        with IQFeedBarDataListener(minibatch=2) as listener, listener.bar_batch_provider() as provider:
            e1 = threading.Event()

            listener.on_bar += lambda event: [self.assertEqual(event['data']['Symbol'], 'SPY'), e1.set()]

            e2 = threading.Event()

            listener.on_bar_batch += lambda event: [self.assertEqual(event['data']['Symbol'][0], 'SPY'), e2.set()]

            listener.watch(symbol='SPY', interval_len=5, interval_type='s', update=1, lookback_bars=10)

            e1.wait()
            e2.wait()

            for i, d in enumerate(provider):
                self.assertEqual(d.shape, (2, 9))
                self.assertEqual(d['Symbol'][0], 'SPY')
                self.assertNotEqual(d['Time Stamp'][0], d['Time Stamp'][1])

                if i == 1:
                    break

    def test_listener(self):
        with IQFeedBarDataListener(minibatch=2) as listener:
            q = queue.Queue()

            e1 = threading.Event()

            listener.on_bar += lambda event: [self.assertEqual(event['data']['Symbol'], 'SPY'), e1.set()]

            listener.on_bar_batch += lambda event: q.put(event['data'])

            listener.watch(symbol='SPY', interval_len=5, interval_type='s', update=1, lookback_bars=10)

            e1.wait()

            for d in [q.get(), q.get()]:
                self.assertEqual(d.shape, (2, 9))
                self.assertEqual(d['Symbol'][0], 'SPY')
                self.assertNotEqual(d['Time Stamp'][0], d['Time Stamp'][1])

if __name__ == '__main__':
    unittest.main()
