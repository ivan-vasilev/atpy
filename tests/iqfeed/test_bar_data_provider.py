import unittest

from atpy.data.iqfeed.iqfeed_bar_data_provider import *
from pyevents.simple_events import AsyncListeners


class TestIQFeedBarData(unittest.TestCase):
    """
    IQFeed bar data test, which checks whether the class works in basic terms
    """

    def test_provider(self):
        listeners = AsyncListeners()

        with IQFeedBarDataListener(listeners=listeners, mkt_snapshot_depth=3, interval_len=300) as listener:
            # test bars
            e1 = {'GOOG': threading.Event(), 'IBM': threading.Event()}
            counters = {'GOOG': 0, 'IBM': 0}

            def bar_listener(event):
                if event['type'] == 'bar':
                    self.assertTrue(event['data']['symbol'] in ['IBM', 'GOOG'])
                    counters[event['data']['symbol']] += 1
                    if counters[event['data']['symbol']] >= listener.mkt_snapshot_depth:
                        e1[event['data']['symbol']].set()

            listeners += bar_listener

            # test market snapshot
            e3 = threading.Event()

            listeners += lambda event: [self.assertEqual(event['data'].shape[1], 9), e3.set()] if event['type'] == 'bar_market_snapshot' else None

            listeners({'type': 'watch_bars', 'data': {'symbol': ['GOOG', 'IBM'], 'update': 1}})

            for e in e1.values():
                e.wait()

            listeners({'type': 'request_market_snapshot_bars'})

            e3.wait()

    def test_listener(self):
        listeners = AsyncListeners()

        with IQFeedBarDataListener(listeners=listeners, interval_len=300) as listener:
            e1 = threading.Event()

            listeners += lambda event: [self.assertEqual(event['data']['symbol'], 'SPY'), e1.set()] if event['type'] == 'bar' else None

            listener.watch(symbol='SPY', interval_len=5, interval_type='s', update=1, lookback_bars=10)

            e1.wait()


if __name__ == '__main__':
    unittest.main()
