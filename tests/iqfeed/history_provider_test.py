import unittest
from atpy.data.iqfeed.iqfeed_history_provider import *


class TestIQFeedHistory(unittest.TestCase):
    """
    IQFeed history provider test, which checks whether the class works in basic terms
    """

    def test_provider(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker="IBM", max_ticks=100)

        with IQFeedHistoryProvider(minibatch=4, filter_provider=filter_provider) as provider:
            for i, d in enumerate(provider):
                if i == 2:
                    break

            self.assertEqual(len(d), 14)

            for v in d.values():
                self.assertEqual(len(v), 4)

if __name__ == '__main__':
    unittest.main()
