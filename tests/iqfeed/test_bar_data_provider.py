import unittest

from atpy.data.iqfeed.iqfeed_bar_data_provider import *


class TestIQFeedBarData(unittest.TestCase):
    """
    IQFeed bar data test, which checks whether the class works in basic terms
    """

    def test_provider(self):
        with IQFeedBarDataListener(mkt_snapshot_depth=3, interval_len=300) as listener:
            # test bars
            e1 = {'GOOG': threading.Event(), 'IBM': threading.Event()}
            counters = {'GOOG': 0, 'IBM': 0}

            def bar_listener(event):
                self.assertTrue(event['data']['symbol'] in ['IBM', 'GOOG'])
                counters[event['data']['symbol']] += 1
                if counters[event['data']['symbol']] >= listener.mkt_snapshot_depth:
                    e1[event['data']['symbol']].set()

            listener.on_bar += bar_listener

            # test market snapshot
            e3 = threading.Event()

            listener.on_market_snapshot += lambda event: [self.assertEqual(event['data'].shape[1], 9), e3.set()]

            mkt_snapshot = events.after(lambda: {'type': 'request_market_snapshot_bars'})
            mkt_snapshot += listener.on_event

            watch_bars = events.after(lambda: {'type': 'watch_bars', 'data': {'symbol': ['GOOG', 'IBM'], 'update': 1}})
            watch_bars += listener.on_event
            watch_bars()

            for e in e1.values():
                e.wait()

            mkt_snapshot()

            e3.wait()

    def test_listener(self):
        with IQFeedBarDataListener(interval_len=300) as listener:
            e1 = threading.Event()

            listener.on_bar += lambda event: [self.assertEqual(event['data']['symbol'], 'SPY'), e1.set()]

            listener.watch(symbol='SPY', interval_len=5, interval_type='s', update=1, lookback_bars=10)

            e1.wait()

if __name__ == '__main__':
    unittest.main()
