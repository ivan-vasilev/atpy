import unittest
from atpy.data.iqfeed.iqfeed_news_provider import *


class TestIQFeedNews(unittest.TestCase):
    """
    IQFeed news test, which checks whether the class works in basic terms
    """

    def test_provider(self):
        filter_provider = DefaultNewsFilterProvider()
        filter_provider += NewsFilter(symbols=['AAPL'])
        filter_provider += NewsFilter(symbols=['IBM'])

        with IQFeedNewsProvider(attach_text=True, minibatch=3, filter_provider=filter_provider) as news_provider:
            for i, d in enumerate(news_provider):
                if i == 1:
                    break

            self.assertEqual(len(d), 6)
            self.assertEqual(len(d['text']), 3)
            self.assertGreater(len(d['text'][0]), 0)
            self.assertTrue('AAPL' in d['symbols'][0] or 'IBM' in d['symbols'][0])

if __name__ == '__main__':
    unittest.main()
