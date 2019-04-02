import threading
import unittest

from atpy.data.iqfeed.iqfeed_bar_data_provider import *
from pyevents.events import AsyncListeners
from atpy.data.util import get_nasdaq_listed_companies

logging.basicConfig(level=logging.DEBUG)


class TestIQFeedBarData(unittest.TestCase):
    """
    IQFeed bar data test, which checks whether the class works in basic terms
    """

    def test_provider(self):
        listeners = AsyncListeners()

        with IQFeedBarDataListener(listeners=listeners, mkt_snapshot_depth=10, interval_len=60) as listener:
            # test bars
            e1 = {'GOOG': threading.Event(), 'IBM': threading.Event()}

            def bar_listener(event):
                if event['type'] == 'bar':
                    df = event['data']
                    symbol = df.index[0][1]
                    self.assertTrue(symbol in ['IBM', 'GOOG'])
                    self.assertEqual(len(df), listener.mkt_snapshot_depth)
                    e1[symbol].set()

            listeners += bar_listener

            # test market snapshot
            e3 = threading.Event()

            listeners += lambda event: [self.assertEqual(event['data'].shape[1], 9), e3.set()] if event['type'] == 'bar_market_snapshot' else None

            listeners({'type': 'watch_bars', 'data': {'symbol': ['GOOG', 'IBM'], 'update': 1}})

            for e in e1.values():
                e.wait()

    def test_listener(self):
        listeners = AsyncListeners()

        with IQFeedBarDataListener(listeners=listeners, interval_len=300, interval_type='s', mkt_snapshot_depth=10) as listener:
            e1 = threading.Event()

            listeners += lambda event: [self.assertEqual(event['data'].index[0][1], 'SPY'), e1.set()] if event['type'] == 'bar' else None

            listener.watch_bars(symbol='SPY')

            e1.wait()

    def test_performance(self):
        listeners = AsyncListeners()
        import time
        nasdaq = get_nasdaq_listed_companies()
        nasdaq = nasdaq.loc[nasdaq['Market Category'] == 'Q']
        nasdaq = nasdaq.sample(400)
        with IQFeedBarDataListener(listeners=listeners, mkt_snapshot_depth=200, interval_len=60, interval_type='s', adjust_history=False, update_interval=1) as listener:
            listener.watch_bars(nasdaq['Symbol'].to_list())
            time.sleep(1000)

    def test_partial_updates(self):
        listeners = AsyncListeners()
        import time
        with IQFeedBarDataListener(listeners=listeners, mkt_snapshot_depth=10, interval_len=60, interval_type='s', adjust_history=False, update_interval=1) as listener:
            listener.watch_bars('TSLA')
            time.sleep(1000)


if __name__ == '__main__':
    unittest.main()
