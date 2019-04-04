import unittest
from collections import OrderedDict

from atpy.data.iqfeed.iqfeed_level_1_provider import *
from atpy.data.util import get_nasdaq_listed_companies
from pyevents.events import *


class TestIQFeedLevel1(unittest.TestCase):
    """
    IQFeed streaming news test, which checks whether the class works in basic terms
    """

    def test_fundamentals(self):
        listeners = AsyncListeners()
        with IQFeedLevel1Listener(listeners=listeners) as listener:
            ffilter = listener.fundamentals_filter()

            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('MSFT')
            listener.watch('SPY')
            listener.request_watches()
            e1 = threading.Event()

            def on_fund_item(fund):
                try:
                    self.assertTrue(fund['symbol'] in {'SPY', 'AAPL', 'IBM', 'GOOG', 'MSFT'})
                    self.assertEqual(len(fund), 50)
                finally:
                    e1.set()

            ffilter += on_fund_item

            e1.wait()

    def test_get_fundamentals(self):
        funds = get_fundamentals({'TRC', 'IBM', 'AAPL', 'GOOG', 'MSFT'})
        self.assertTrue('AAPL' in funds and 'IBM' in funds and 'GOOG' in funds and 'MSFT' in funds and 'TRC' in funds)
        for _, v in funds.items():
            self.assertGreater(len(v), 0)

    def test_update_summary(self):
        listeners = AsyncListeners()

        with IQFeedLevel1Listener(listeners=listeners) as listener:
            e1 = threading.Event()

            def on_summary_item(data):
                try:
                    self.assertEqual(len(data), 16)
                finally:
                    e1.set()

            summary_filter = listener.level_1_summary_filter()
            summary_filter += on_summary_item

            e2 = threading.Event()

            def on_update_item(data):
                try:
                    self.assertEqual(len(data), 16)
                finally:
                    e2.set()

            update_filter = listener.level_1_update_filter()
            update_filter += on_update_item

            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('MSFT')
            listener.watch('SPY')

            e1.wait()
            e2.wait()

    def test_update_summary_deque(self):
        listeners = SyncListeners()

        mkt_snapshot_depth = 100
        with IQFeedLevel1Listener(listeners=listeners, mkt_snapshot_depth=mkt_snapshot_depth) as listener:
            e1 = threading.Event()

            def on_summary_item(data):
                try:
                    self.assertEqual(len(data), 16)
                    self.assertTrue(isinstance(data, OrderedDict))
                finally:
                    e1.set()

            summary_filter = listener.level_1_summary_filter()
            summary_filter += on_summary_item

            e2 = threading.Event()

            def on_update_item(data):
                try:
                    self.assertEqual(len(data), 16)
                    self.assertTrue(isinstance(data, OrderedDict))
                    self.assertGreater(len(next(iter(data.values()))), 1)
                finally:
                    e2.set()

            update_filter = listener.level_1_update_filter()
            update_filter += on_update_item

            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('MSFT')
            listener.watch('SPY')

            e1.wait()
            e2.wait()

    def test_update_summary_deque_trades_only(self):
        listeners = SyncListeners()

        mkt_snapshot_depth = 10
        with IQFeedLevel1Listener(listeners=listeners, mkt_snapshot_depth=mkt_snapshot_depth) as listener:
            e1 = threading.Event()

            def on_summary_item(data):
                try:
                    self.assertEqual(len(data), 16)
                    self.assertTrue(isinstance(data, OrderedDict))
                finally:
                    e1.set()

            summary_filter = listener.level_1_summary_filter()
            summary_filter += on_summary_item

            e2 = threading.Event()

            def on_update_item(data):
                try:
                    self.assertEqual(len(data), 16)
                    self.assertTrue(isinstance(data, OrderedDict))
                    self.assertGreater(len(next(iter(data.values()))), 1)
                    trade_times = data['most_recent_trade_time']
                    self.assertEqual(len(trade_times), len(set(trade_times)))
                finally:
                    if len(trade_times) == mkt_snapshot_depth:
                        e2.set()

            update_filter = listener.level_1_update_filter()
            update_filter += on_update_item

            listener.watch_trades('IBM')
            listener.watch_trades('AAPL')
            listener.watch_trades('GOOG')
            listener.watch_trades('MSFT')
            listener.watch_trades('SPY')

            e1.wait()
            e2.wait()

    def test_news(self):
        listeners = AsyncListeners()

        with IQFeedLevel1Listener(listeners=listeners) as listener:
            e1 = threading.Event()

            def on_news_item(news_item):
                try:
                    self.assertEqual(len(news_item), 6)
                    self.assertGreater(len(news_item['headline']), 0)
                finally:
                    e1.set()

            news_filter = listener.news_filter()
            news_filter += on_news_item

            listener.news_on()
            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('SPY')

            e1.wait()

    # @unittest.skip('Run manually')
    def test_nasdaq_quotes_and_trades_performance(self):
        logging.basicConfig(level=logging.DEBUG)

        listeners = AsyncListeners()
        import time
        nasdaq = get_nasdaq_listed_companies()
        nasdaq = nasdaq.loc[nasdaq['Market Category'] == 'Q']
        nasdaq = nasdaq.sample(480)
        with IQFeedLevel1Listener(listeners=listeners, mkt_snapshot_depth=200) as listener:
            listener.watch(nasdaq['Symbol'].to_list())
            time.sleep(1000)

    # @unittest.skip('Run manually')
    def test_nasdaq_trades_performance(self):
        logging.basicConfig(level=logging.DEBUG)

        listeners = AsyncListeners()
        import time
        nasdaq = get_nasdaq_listed_companies()
        nasdaq = nasdaq.loc[nasdaq['Market Category'] == 'Q']
        nasdaq = nasdaq.sample(480)
        with IQFeedLevel1Listener(listeners=listeners, mkt_snapshot_depth=200) as listener:
            listener.watch_trades(nasdaq['Symbol'].to_list())
            time.sleep(1000)

    # @unittest.skip('Run manually')
    def test_nyse_quotes_and_trades_performance(self):
        logging.basicConfig(level=logging.DEBUG)

        listeners = AsyncListeners()
        import time
        nyse = pd.read_csv('/home/ivan/Downloads/cik_ticker.csv', sep='|')
        nyse = nyse.loc[nyse['Exchange'] == 'NYSE']
        nyse = nyse.sample(480)
        with IQFeedLevel1Listener(listeners=listeners, mkt_snapshot_depth=200) as listener:
            listener.watch(nyse['Ticker'].to_list())
            time.sleep(1000)


if __name__ == '__main__':
    unittest.main()
