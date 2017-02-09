import unittest

from atpy.data.iqfeed.iqfeed_history_provider import *


class TestIQFeedHistory(unittest.TestCase):
    """
    IQFeed history provider test, which checks whether the class works in basic terms
    """

    def test_provider_column_mode(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker="IBM", max_ticks=20)
        provider = IQFeedHistoryProvider(minibatch=4, filter_provider=filter_provider)

        with provider:
            e1 = threading.Event()

            def process_batch_listener_column(event):
                batch = event.data
                self.assertEqual(len(batch), 14)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 20)
                e1.set()

            provider.process_batch += process_batch_listener_column

            e2 = threading.Event()

            def process_minibatch_listener_column(event):
                batch = event.data
                self.assertEqual(len(batch), 14)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 4)
                e2.set()

            provider.process_minibatch += process_minibatch_listener_column

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 14)

                for v in d.values():
                    self.assertEqual(len(v), 4)

                self.assertNotEqual(d['tick_id'][0], d['tick_id'][1])

                if i == 1:
                    break

            e1.wait()
            e2.wait()

    def test_provider_row_mode(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker="IBM", max_ticks=20)

        provider = IQFeedHistoryProvider(minibatch=4, filter_provider=filter_provider, column_mode=False)

        with provider:
            e1 = threading.Event()

            def process_batch_listener(event):
                batch = event.data
                self.assertEqual(len(batch[0]), 14)
                self.assertEqual(len(batch), 20)
                e1.set()

            provider.process_batch += process_batch_listener

            e2 = threading.Event()

            def process_minibatch_listener(event):
                batch = event.data
                self.assertEqual(len(batch[0]), 14)
                self.assertEqual(len(batch), 4)
                e2.set()

            provider.process_minibatch += process_minibatch_listener

            e1.wait()
            e2.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 4)

                for item in d:
                    self.assertEqual(len(item), 14)

                self.assertNotEqual(d[0]['tick_id'], d[1]['tick_id'])

                if i == 1:
                    break

if __name__ == '__main__':
    unittest.main()
