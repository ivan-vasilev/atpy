import threading
import unittest

from pandas.util.testing import assert_frame_equal

from atpy.data.iqfeed.iqfeed_bar_data_provider import *
from atpy.data.util import get_nasdaq_listed_companies
from pyevents.events import AsyncListeners, SyncListeners


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
                    symbol_ind = df.index.names.index('symbol')
                    symbol = df.index[symbol_ind][1]
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

    def test_multiple_intervals(self):
        listeners = SyncListeners()

        with    IQFeedBarDataListener(listeners=listeners, mkt_snapshot_depth=10, interval_len=30, update_interval=1) as listener_30, \
                IQFeedBarDataListener(listeners=listeners, mkt_snapshot_depth=10, interval_len=60, update_interval=1) as listener_60, \
                IQFeedBarDataListener(listeners=listeners, mkt_snapshot_depth=10, interval_len=120, update_interval=1) as listener_120:

            e1 = {'GOOG': threading.Event(), 'FB': threading.Event(), 'AAPL': threading.Event(), 'TSLA': threading.Event()}

            def bar_listener(event):
                self.assertTrue(isinstance(event, dict))
                self.assertEqual(len(event), 3)

                symbols = set()
                timestamps = list()

                for k, df in event.items():
                    self.assertFalse(df.empty)
                    symbol_ind = df.index.names.index('symbol')
                    symbol = df.index[0][symbol_ind]
                    symbols.add(symbol)

                    timestamp_ind = df.index.names.index('timestamp')
                    timestamps.append(df.index.levels[timestamp_ind])

                self.assertFalse((timestamps[0] == timestamps[1]).min())
                self.assertFalse((timestamps[0] == timestamps[2]).min())
                self.assertEqual(len(symbols), 1)

                e1[symbol].set()

            mbars_filter = MultiIntervalBarsListener(listeners,
                                                     {'30s': listener_30.all_full_bars_event_stream(),
                                                      '60s': listener_60.all_full_bars_event_stream(),
                                                      '120s': listener_120.all_full_bars_event_stream()}) \
                .event_filter()

            mbars_filter += bar_listener

            listeners({'type': 'watch_bars', 'data': {'symbol': ['GOOG', 'FB', 'AAPL', 'TSLA'], 'update': 1}})

            for e in e1.values():
                e.wait()

    def test_listener(self):
        listeners = AsyncListeners()

        with IQFeedBarDataListener(listeners=listeners, interval_len=300, interval_type='s', mkt_snapshot_depth=10) as listener:
            e1 = threading.Event()
            full_bars_filter = listener.all_full_bars_event_stream()
            full_bars_filter += lambda data: [self.assertEqual(data.index[0][1], 'SPY'), e1.set()]

            e2 = threading.Event()
            updates_filter = listener.all_full_bars_event_stream()
            updates_filter += lambda data: [self.assertEqual(data.index[0][1], 'SPY'), e2.set()]

            listener.watch_bars(symbol='SPY')

            e1.wait()
            e2.wait()

    def test_correctness_small(self):
        self._test_correctness('IBM')

    def test_nasdaq_correctness_large(self):
        nasdaq = get_nasdaq_listed_companies()
        nasdaq = nasdaq.loc[nasdaq['Market Category'] == 'Q']
        nasdaq = nasdaq.sample(400)

        self._test_correctness(nasdaq['Symbol'].to_list())

    def _test_correctness(self, symbols):
        logging.basicConfig(level=logging.DEBUG)

        listeners = SyncListeners()
        depth = 5
        with IQFeedBarDataListener(listeners=listeners, mkt_snapshot_depth=depth, interval_len=60, interval_type='s', adjust_history=False, update_interval=1) as listener:
            dfs = dict()

            te = threading.Event()

            def full_bar_listener(df):
                self.assertEqual(df.shape[0], depth)
                symbol_ind = df.index.names.index('symbol')
                symbol = df.index.levels[symbol_ind][0]
                dfs[symbol] = df.copy(deep=True)

            full_bars = listener.all_full_bars_event_stream()
            full_bars += full_bar_listener

            conditions = {'ind_equal': False, 'ind_not_equal': False}

            def bar_update_listener(df):
                self.assertEqual(df.shape[0], depth)
                symbol_ind = df.index.names.index('symbol')
                symbol = df.index.levels[symbol_ind][0]
                old_df = dfs[symbol]

                if old_df.index.equals(df.index):
                    assert_frame_equal(old_df.iloc[:-1], df.iloc[:-1], check_index_type=False)
                    conditions['ind_equal'] = True
                else:
                    assert_frame_equal(old_df.iloc[1:], df.iloc[:-1], check_index_type=False)
                    conditions['ind_not_equal'] = True

                try:
                    assert_frame_equal(old_df.iloc[-1:], df.iloc[-1:], check_index_type=False)
                except AssertionError:
                    pass
                else:
                    raise AssertionError

                if conditions['ind_equal'] is True and conditions['ind_not_equal'] is True:
                    te.set()

            bar_updates = listener.bar_updates_event_stream()
            bar_updates += bar_update_listener

            listener.watch_bars(symbols)

            te.wait()

    @unittest.skip('Run manually')
    def test_nasdaq_performance(self):
        listeners = AsyncListeners()
        import time
        nasdaq = get_nasdaq_listed_companies()
        nasdaq = nasdaq.loc[nasdaq['Market Category'] == 'Q']
        nasdaq = nasdaq.sample(480)
        with IQFeedBarDataListener(listeners=listeners, mkt_snapshot_depth=200, interval_len=1, interval_type='s', adjust_history=False) as listener:
            listener.watch_bars(nasdaq['Symbol'].to_list())
            time.sleep(1000)


if __name__ == '__main__':
    unittest.main()
