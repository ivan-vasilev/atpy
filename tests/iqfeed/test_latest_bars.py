import unittest

from atpy.data.iqfeed.iqfeed_latest_bars import *
import pandas as pd


class TestIQFeedBarData(unittest.TestCase):
    """
    IQFeed bar data snapshot test, which checks whether the class works in basic terms
    """

    def test_1(self):
        with IQFeedLatestBars(mkt_snapshot_depth=3, interval_len=300) as listener:
            # test bars
            e1 = {'GOOG': threading.Event(), 'IBM': threading.Event()}
            counters = {'GOOG': 0, 'IBM': 0}

            def bar_listener(event):
                self.assertTrue(event['data']['Symbol'] in ['IBM', 'GOOG'])
                counters[event['data']['Symbol']] += 1
                if counters[event['data']['Symbol']] >= listener.mkt_snapshot_depth:
                    e1[event['data']['Symbol']].set()

            listener.on_bar += bar_listener

            # test market snapshot
            e2 = threading.Event()
            e3 = threading.Event()

            def on_market_snapshot(event):
                self.assertEqual(event['data'].shape[1], 9)
                e2.set()
                if len(event['data'].index.levels[0]) == 4:
                    e3.set()

            listener.on_market_snapshot += on_market_snapshot

            mkt_snapshot = events.after(lambda: {'type': 'request_market_snapshot_bars'})
            mkt_snapshot += listener.on_event

            watch_bars = events.after(lambda: {'type': 'watch_bars', 'data': {'symbol': ['GOOG', 'IBM'], 'update': 1}})
            watch_bars += listener.on_event
            watch_bars()

            watch_history_bars = events.after(lambda: {'type': 'watch_history_bars', 'data': ['AAPL', 'FB']})
            watch_history_bars += listener.on_event
            watch_history_bars()

            for e in e1.values():
                e.wait()

            mkt_snapshot()

            e2.wait()
            e3.wait()

    def test_2(self):
        with IQFeedLatestBars(mkt_snapshot_depth=3, interval_len=300) as listener:
            df = pd.read_csv('/home/hok/Downloads/all_stocks_1yr.csv')

            # test bars
            ticker = df['Name'].unique()
            ticker.sort()
            ticker = list(ticker[:5])

            e1 = {t: threading.Event() for t in ticker}
            counters = {t: 0 for t in ticker}
            counters['Total'] = 0

            def bar_listener(event):
                self.assertTrue(event['data']['Symbol'] in ticker)
                counters[event['data']['Symbol']] += 1
                counters['Total'] += 1
                print(counters['Total'])
                if counters[event['data']['Symbol']] >= listener.mkt_snapshot_depth:
                    e1[event['data']['Symbol']].set()

            listener.on_bar += bar_listener

            # test market snapshot
            e2 = threading.Event()
            e3 = threading.Event()

            def on_market_snapshot(event):
                self.assertEqual(event['data'].shape[1], 9)
                print(event['data'].index)
                e2.set()
                if len(event['data'].index.levels[0]) >= len(ticker):
                    e3.set()

            listener.on_market_snapshot += on_market_snapshot

            mkt_snapshot = events.after(lambda: {'type': 'request_market_snapshot_bars'})
            mkt_snapshot += listener.on_event

            watch_bars = events.after(lambda: {'type': 'watch_bars', 'data': {'symbol': ticker, 'update': 1}})
            watch_bars += listener.on_event
            watch_bars()

            watch_history_bars = events.after(lambda: {'type': 'watch_history_bars', 'data': ['AAPL', 'FB']})
            watch_history_bars += listener.on_event
            watch_history_bars()

            for e in e1.values():
                e.wait()

            # while True:
            # mkt_snapshot()

            e2.wait()
            e3.wait()

if __name__ == '__main__':
    unittest.main()
