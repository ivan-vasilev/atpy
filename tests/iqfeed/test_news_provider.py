import unittest

from atpy.data.iqfeed.iqfeed_news_provider import *


class TestIQFeedNews(unittest.TestCase):
    """
    IQFeed news test, which checks whether the class works in basic terms
    """

    def test_provider_column_mode(self):
        filter_provider = DefaultNewsFilterProvider()
        filter_provider += NewsFilter(symbols=['AAPL'], limit=10)
        filter_provider += NewsFilter(symbols=['IBM'], limit=10)

        with IQFeedMewsProvider(attach_text=True, minibatch=3, filter_provider=filter_provider, column_mode=True) as provider:
            def process_batch_listener(event):
                batch = event.data
                self.assertEqual(len(list(batch.keys())), 7)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 10)

            provider.process_batch += process_batch_listener

            def process_minibatch_listener(event):
                batch = event.data
                self.assertEqual(len(list(batch.keys())), 7)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 3)

            provider.process_minibatch += process_minibatch_listener

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 7)
                self.assertEqual(len(d['text']), 3)
                self.assertGreater(len(d['text'][0]), 0)
                self.assertTrue('AAPL' in d['symbol_list'][0] or 'IBM' in d['symbol_list'][0])

                if i == 1:
                    break

    def test_provider_row_mode(self):
        filter_provider = DefaultNewsFilterProvider()
        filter_provider += NewsFilter(symbols=['AAPL'], limit=10)
        filter_provider += NewsFilter(symbols=['IBM'], limit=10)

        with IQFeedMewsProvider(attach_text=True, minibatch=3, filter_provider=filter_provider, column_mode=False) as provider:
            def process_batch_listener(event):
                batch = event.data
                self.assertEqual(len(batch), 10)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 7)

            provider.process_batch += process_batch_listener

            def process_minibatch_listener(event):
                batch = event.data
                self.assertEqual(len(batch), 3)
                self.assertEqual(len(list(batch[0].keys())), 7)

            provider.process_minibatch += process_minibatch_listener

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 3)
                self.assertEqual(len(list(d[0].keys())), 7)
                self.assertGreater(len(d[0]['text']), 0)
                self.assertTrue('AAPL' in d[0]['symbol_list'] or 'IBM' in d[0]['symbol_list'])

                if i == 1:
                    break


if __name__ == '__main__':
    unittest.main()
