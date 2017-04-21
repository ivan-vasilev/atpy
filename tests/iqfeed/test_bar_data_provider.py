import unittest

from atpy.data.iqfeed.iqfeed_bar_data_provider import *


class TestIQFeedBarData(unittest.TestCase):
    """
    IQFeed bar data test, which checks whether the class works in basic terms
    """

    def test_provider(self):
        with IQFeedBarDataListener(minibatch=2, mkt_snapshot_minibatch=101) as listener, listener.bar_batch_provider() as provider:
            lookback_count = 100

            # test bars
            e1 = {'GOOG': threading.Event(), 'IBM': threading.Event()}
            counters = {'GOOG': 0, 'IBM': 0}

            def bar_listener(event):
                self.assertTrue(event['data']['Symbol'] in ['IBM', 'GOOG'])
                counters[event['data']['Symbol']] += 1
                if counters[event['data']['Symbol']] >= lookback_count:
                    e1[event['data']['Symbol']].set()

            listener.on_bar += bar_listener

            # test batches
            e2 = threading.Event()

            listener.on_bar_batch += lambda event: [self.assertTrue(event['data']['Symbol'][0] in ['IBM', 'GOOG']), e2.set()]

            # test market snapshot
            e3 = threading.Event()

            listener.on_market_snapshot += lambda event: [self.assertEqual(event['data'].shape, (2, 9)), e3.set()]

            mkt_snapshot = events.after(lambda: {'type': 'request_market_snapshot_bars'})
            mkt_snapshot += listener.on_event

            # test market snapshot minibatch
            e4 = threading.Event()

            def mb_mkt_snapshot_listener(event):
                try:
                    self.assertEqual(event['data'].shape, (2, 101, 9))
                finally:
                    e4.set()

            listener.on_mb_market_snapshot += mb_mkt_snapshot_listener

            watch_bars = events.after(lambda: {'type': 'watch_bars', 'data': {'symbol': ['GOOG', 'IBM'], 'interval_len': 300, 'interval_type': 's', 'update': 1, 'lookback_bars': lookback_count}})
            watch_bars += listener.on_event
            watch_bars()

            for e in e1.values():
                e.wait()

            mkt_snapshot()

            e2.wait()
            e3.wait()
            e4.wait()

            for i, d in enumerate(provider):
                self.assertEqual(d.shape, (2, 9))
                self.assertTrue(d['Symbol'][0] in ['IBM', 'GOOG'])
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
