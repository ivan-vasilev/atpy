import unittest

from atpy.data.iqfeed.iqfeed_level_1_provider import *
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

            def on_update_item(data):
                try:
                    self.assertEqual(len(data), 16)
                finally:
                    e1.set()

            update_filter = listener.level_1_update_filter()
            update_filter += on_update_item

            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('MSFT')
            listener.watch('SPY')

            e1.wait()

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


if __name__ == '__main__':
    unittest.main()
