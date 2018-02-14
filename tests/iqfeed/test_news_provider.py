import unittest

from atpy.data.iqfeed.iqfeed_news_provider import *
from pyevents.events import AsyncListeners


class TestIQFeedNews(unittest.TestCase):
    """
    IQFeed news test, which checks whether the class works in basic terms
    """

    def test_provider(self):
        filter_provider = DefaultNewsFilterProvider()
        filter_provider += NewsFilter(symbols=['AAPL'], limit=10)

        listeners = AsyncListeners()

        with IQFeedNewsListener(listeners=listeners, attach_text=True, filter_provider=filter_provider) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_batch_listener(event):
                if event['type'] == 'news_batch':
                    batch = event['data']
                    self.assertEqual(len(batch), 10)
                    self.assertEqual(len(batch.columns), 6)
                    self.assertTrue('text' in batch.columns)
                    self.assertTrue('AAPL' in batch['symbol_list'][0] or 'IBM' in batch['symbol_list'][0])

                    e1.set()

            listeners += process_batch_listener

            e1.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 10)
                self.assertEqual(len(d.columns), 6)
                self.assertTrue('text' in d.columns)
                self.assertTrue('AAPL' in d['symbol_list'][0] or 'IBM' in d['symbol_list'][0])

                if i == 1:
                    break


if __name__ == '__main__':
    unittest.main()
