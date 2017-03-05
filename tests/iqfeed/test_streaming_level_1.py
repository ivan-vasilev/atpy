import unittest

from atpy.data.iqfeed.iqfeed_level_1_provider import *


class TestIQFeedLevel1(unittest.TestCase):
    """
    IQFeed streaming news test, which checks whether the class works in basic terms
    """

    def test_fundamentals_column_mode(self):
        with IQFeedLevel1Listener(minibatch=2, column_mode=True) as listener, listener.fundamentals_provider() as provider:
            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('MSFT')
            listener.watch('SPY')
            listener.request_watches()
            e1 = threading.Event()

            def on_fund_item(event):
                self.assertEqual(len(event['data']), 50)
                e1.set()

            listener.on_fundamentals += on_fund_item

            e2 = threading.Event()

            def on_fund_mb(event):
                fund_item = event['data']
                self.assertEqual(len(fund_item), 50)
                self.assertEqual(len(fund_item['Symbol']), 2)
                self.assertTrue('SPY' in fund_item['Symbol'] or 'AAPL' in fund_item['Symbol'] or 'IBM' in fund_item['Symbol'] or 'GOOG' in fund_item['Symbol'] or 'MSFT' in fund_item['Symbol'])
                e2.set()

            listener.on_fundamentals_mb += on_fund_mb

            e1.wait()
            e2.wait()

            for i, fund_item in enumerate(provider):
                self.assertEqual(len(fund_item), 50)
                self.assertEqual(len(fund_item['Symbol']), 2)
                self.assertTrue('SPY' in fund_item['Symbol'] or 'AAPL' in fund_item['Symbol'] or 'IBM' in fund_item['Symbol'] or 'GOOG' in fund_item['Symbol'] or 'MSFT' in fund_item['Symbol'])

                if i == 1:
                    break

    def test_fundamentals_row_mode(self):
        with IQFeedLevel1Listener(minibatch=2, column_mode=False) as listener, listener.fundamentals_provider() as provider:
            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('MSFT')
            listener.watch('SPY')

            e1 = threading.Event()

            def on_fund_item(event):
                self.assertEqual(len(event['data']), 50)
                e1.set()

            listener.on_fundamentals += on_fund_item

            e2 = threading.Event()

            def on_fund_mb(event):
                fund_item = event['data']
                self.assertEqual(len(fund_item), 2)
                self.assertEqual(len(fund_item[0]), 50)

                symbols = [fund_item[0]['Symbol'], fund_item[1]['Symbol']]
                self.assertTrue('SPY' in symbols or 'AAPL' in symbols or 'IBM' in symbols or 'GOOG' in symbols or 'MSFT' in symbols)
                e2.set()

            listener.on_fundamentals_mb += on_fund_mb

            e1.wait()
            e2.wait()

            for i, fund_item in enumerate(provider):
                self.assertEqual(len(fund_item), 2)
                self.assertEqual(len(fund_item[0]), 50)
                symbols = [fund_item[0]['Symbol'], fund_item[1]['Symbol']]
                self.assertTrue('SPY' in symbols or 'AAPL' in symbols or 'IBM' in symbols or 'GOOG' in symbols or 'MSFT' in symbols)

                if i == 1:
                    break

    def test_summary_column_mode(self):
        with IQFeedLevel1Listener(minibatch=2, column_mode=True) as listener, listener.summary_provider() as data_provider:
            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('MSFT')
            listener.watch('SPY')

            e1 = threading.Event()

            def on_summary_item(event):
                self.assertEqual(len(event['data']), 16)
                e1.set()

            listener.on_summary += on_summary_item

            e2 = threading.Event()

            def on_summary_mb(event):
                summary_item = event['data']
                self.assertEqual(len(summary_item), 16)
                self.assertEqual(len(summary_item['Symbol']), 2)
                self.assertTrue('SPY' in summary_item['Symbol'] or 'AAPL' in summary_item['Symbol'] or 'IBM' in summary_item['Symbol'] or 'GOOG' in summary_item['Symbol'] or 'MSFT' in summary_item['Symbol'])
                e2.set()

            listener.on_summary_mb += on_summary_mb

            e1.wait()
            e2.wait()

            for i, summary_item in enumerate(data_provider):
                self.assertEqual(len(summary_item), 16)
                self.assertEqual(len(summary_item['Symbol']), 2)
                self.assertTrue('SPY' in summary_item['Symbol'] or 'AAPL' in summary_item['Symbol'] or 'IBM' in summary_item['Symbol'] or 'GOOG' in summary_item['Symbol'] or 'MSFT' in summary_item['Symbol'])

                if i == 1:
                    break

    def test_summary_row_mode(self):
        with IQFeedLevel1Listener(minibatch=2, column_mode=False) as listener, listener.summary_provider() as data_provider:
            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('MSFT')
            listener.watch('SPY')

            e1 = threading.Event()

            def on_summary_item(event):
                self.assertEqual(len(event['data']), 16)
                e1.set()

            listener.on_summary += on_summary_item

            e2 = threading.Event()

            def on_summary_mb(event):
                summary_item = event['data']
                self.assertEqual(len(summary_item), 2)
                self.assertEqual(len(summary_item[0]), 16)

                symbols = [summary_item[0]['Symbol'], summary_item[1]['Symbol']]
                self.assertTrue('SPY' in symbols or 'AAPL' in symbols or 'IBM' in symbols or 'GOOG' in symbols or 'MSFT' in symbols)
                e2.set()

            listener.on_summary_mb += on_summary_mb

            e1.wait()
            e2.wait()

            for i, summary_item in enumerate(data_provider):
                self.assertEqual(len(summary_item), 2)
                self.assertEqual(len(summary_item[0]), 16)
                symbols = [summary_item[0]['Symbol'], summary_item[1]['Symbol']]
                self.assertTrue('SPY' in symbols or 'AAPL' in symbols or 'IBM' in symbols or 'GOOG' in symbols or 'MSFT' in symbols)

                if i == 1:
                    break

    def test_update_column_mode(self):
        with IQFeedLevel1Listener(minibatch=2, column_mode=True) as listener, listener.update_provider() as update_provider:
            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('MSFT')
            listener.watch('SPY')

            e1 = threading.Event()

            def on_update_item(event):
                self.assertEqual(len(event['data']), 16)
                e1.set()

            listener.on_update += on_update_item

            e2 = threading.Event()

            def on_update_mb(event):
                update_item = event['data']
                self.assertEqual(len(update_item), 16)
                self.assertEqual(len(update_item['Symbol']), 2)
                self.assertTrue('SPY' in update_item['Symbol'] or 'AAPL' in update_item['Symbol'] or 'IBM' in update_item['Symbol'] or 'GOOG' in update_item['Symbol'] or 'MSFT' in update_item['Symbol'])
                e2.set()

            listener.on_update_mb += on_update_mb

            e1.wait()
            e2.wait()

            for i, update_item in enumerate(update_provider):
                self.assertEqual(len(update_item), 16)
                self.assertEqual(len(update_item['Symbol']), 2)
                self.assertTrue('SPY' in update_item['Symbol'] or 'AAPL' in update_item['Symbol'] or 'IBM' in update_item['Symbol'] or 'GOOG' in update_item['Symbol'] or 'MSFT' in update_item['Symbol'])

                if i == 1:
                    break

    def test_update_row_mode(self):
        with IQFeedLevel1Listener(minibatch=2, column_mode=False) as listener, listener.update_provider() as provider:
            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('MSFT')
            listener.watch('SPY')

            e1 = threading.Event()

            def on_update_item(event):
                self.assertEqual(len(event['data']), 16)
                e1.set()

            listener.on_update += on_update_item

            e2 = threading.Event()

            def on_update_mb(event):
                update_item = event['data']
                self.assertEqual(len(update_item), 2)
                self.assertEqual(len(update_item[0]), 16)

                symbols = [update_item[0]['Symbol'], update_item[1]['Symbol']]
                self.assertTrue('SPY' in symbols or 'AAPL' in symbols or 'IBM' in symbols or 'GOOG' in symbols or 'MSFT' in symbols)
                e2.set()

            listener.on_update_mb += on_update_mb

            e1.wait()
            e2.wait()

            for i, update_item in enumerate(provider):
                self.assertEqual(len(update_item), 2)
                self.assertEqual(len(update_item[0]), 16)
                symbols = [update_item[0]['Symbol'], update_item[1]['Symbol']]
                self.assertTrue('SPY' in symbols or 'AAPL' in symbols or 'IBM' in symbols or 'GOOG' in symbols or 'MSFT' in symbols)

                if i == 1:
                    break

    def test_news_column_mode(self):
        with IQFeedLevel1Listener(minibatch=2) as listener, listener.news_provider() as provider:
            listener.watch('IBM')
            listener.watch('AAPL')
            listener.watch('GOOG')
            listener.watch('SPY')
            listener.news_on()

            e1 = threading.Event()

            def on_news_item(event):
                news_item = event['data']
                self.assertEqual(len(news_item), 6)
                self.assertGreater(len(news_item['headline']), 0)
                e1.set()

            listener.on_news += on_news_item

            e2 = threading.Event()

            def on_news_mb(event):
                news_item = event['data']
                self.assertEqual(len(news_item), 6)
                self.assertEqual(len(news_item['headline']), 2)
                self.assertGreater(len(news_item['headline'][0]), 0)
                self.assertNotEqual(news_item['story_id'][0], news_item['story_id'][1])
                e2.set()

            listener.on_news_mb += on_news_mb

            e1.wait()
            e2.wait()

            for i, news_item in enumerate(provider):
                self.assertEqual(len(news_item), 6)
                self.assertEqual(len(news_item['headline']), 2)
                self.assertGreater(len(news_item['headline'][0]), 0)
                self.assertNotEqual(news_item['story_id'][0], news_item['story_id'][1])

                if i == 1:
                    break

if __name__ == '__main__':
    unittest.main()
