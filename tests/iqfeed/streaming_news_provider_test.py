import unittest
from atpy.data.iqfeed.iqfeed_streaming_news_provider import *


class TestIQFeedStreamingNews(unittest.TestCase):
    """
    IQFeed streaming news test, which checks whether the class works in basic terms
    """

    def setUp(self):
        self.news_provider = IQFeedStreamingNewsProvider()

    def test_provider(self):
        for i, d in enumerate(self.news_provider):
            if i == 2:
                break

        self.assertEqual(len(d), 2)
        self.assertEqual(len(d['headline']), 1)
        self.assertGreater(len(d['headline'][0]), 0)

if __name__ == '__main__':
    unittest.main()
