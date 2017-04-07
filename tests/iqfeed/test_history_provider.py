import unittest
import shutil

from atpy.data.iqfeed.iqfeed_history_provider import *


class TestIQFeedHistory(unittest.TestCase):
    """
    IQFeed history provider test, which checks whether the class works in basic terms
    """

    def test_ticks_column_mode(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker="IBM", max_ticks=20)

        try:
            with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, column_mode=True, lmdb_path='/tmp/test_history_provider_test_ticks_column_mode') as listener, listener.minibatch_provider() as provider:

                e1 = threading.Event()

                def process_tick(event):
                    data = event['data']
                    self.assertEqual(len(list(data.keys())), 15)
                    e1.set()

                listener.process_datum += process_tick

                e2 = threading.Event()

                def process_batch_listener_column(event):
                    batch = event['data']
                    self.assertEqual(len(batch), 15)
                    self.assertEqual(len(batch[list(batch.keys())[0]]), 20)
                    e2.set()

                listener.process_batch += process_batch_listener_column

                e3 = threading.Event()

                def process_minibatch_listener_column(event):
                    batch = event['data']
                    self.assertEqual(len(batch), 15)
                    self.assertEqual(len(batch[list(batch.keys())[0]]), 4)
                    e3.set()

                listener.process_minibatch += process_minibatch_listener_column

                for i, d in enumerate(provider):
                    self.assertEqual(len(d), 15)

                    for v in d.values():
                        self.assertEqual(len(v), 4)

                    self.assertNotEqual(d['TickID'][0], d['TickID'][1])

                    if i == 1:
                        break

                e2.wait()
                e3.wait()
        finally:
            shutil.rmtree('/tmp/test_history_provider_test_ticks_column_mode')

    def test_ticks_row_mode(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker="IBM", max_ticks=20)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, column_mode=False) as listener, listener.minibatch_provider() as provider:
            e1 = threading.Event()

            def process_tick(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 15)
                e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener(event):
                batch = event['data']
                self.assertEqual(len(batch[0]), 15)
                self.assertEqual(len(batch), 20)
                e2.set()

            listener.process_batch += process_batch_listener

            e3 = threading.Event()

            def process_minibatch_listener(event):
                batch = event['data']
                self.assertEqual(len(batch[0]), 15)
                self.assertEqual(len(batch), 4)
                e3.set()

            listener.process_minibatch += process_minibatch_listener

            e2.wait()
            e3.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 4)

                for item in d:
                    self.assertEqual(len(item), 15)

                self.assertNotEqual(d[0]['TickID'], d[1]['TickID'])

                if i == 1:
                    break

    def test_bars_column_mode(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=20)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, column_mode=True) as listener, listener.minibatch_provider() as provider:

            e1 = threading.Event()

            def process_tick(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 10)
                e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener_column(event):
                batch = event['data']
                self.assertEqual(len(batch), 10)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 20)
                e2.set()

            listener.process_batch += process_batch_listener_column

            e3 = threading.Event()

            def process_minibatch_listener_column(event):
                batch = event['data']
                self.assertEqual(len(batch), 10)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 4)
                e3.set()

            listener.process_minibatch += process_minibatch_listener_column

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 10)

                for v in d.values():
                    self.assertEqual(len(v), 4)

                self.assertNotEqual(d['Time'][0], d['Time'][1])

                if i == 1:
                    break

            e2.wait()
            e3.wait()

    def test_bars_row_mode(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=20)

        try:
            with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, column_mode=False, lmdb_path='/tmp/test_history_provider_test_bars_row_mode') as listener, listener.minibatch_provider() as provider:
                e1 = threading.Event()

                def process_tick(event):
                    data = event['data']
                    self.assertEqual(len(list(data.keys())), 10)
                    e1.set()

                listener.process_datum += process_tick

                e2 = threading.Event()

                def process_batch_listener(event):
                    batch = event['data']
                    self.assertEqual(len(batch[0]), 10)
                    self.assertEqual(len(batch), 20)
                    e2.set()

                listener.process_batch += process_batch_listener

                e3 = threading.Event()

                def process_minibatch_listener(event):
                    batch = event['data']
                    self.assertEqual(len(batch[0]), 10)
                    self.assertEqual(len(batch), 4)
                    e3.set()

                listener.process_minibatch += process_minibatch_listener

                e2.wait()
                e3.wait()

                for i, d in enumerate(provider):
                    self.assertEqual(len(d), 4)

                    for item in d:
                        self.assertEqual(len(item), 10)

                    self.assertNotEqual(d[0]['Time'], d[1]['Time'])

                    if i == 1:
                        break
        finally:
            shutil.rmtree('/tmp/test_history_provider_test_bars_row_mode')

    def test_daily_column_mode(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsDailyFilter(ticker="IBM", num_days=20)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, column_mode=True) as listener, listener.minibatch_provider() as provider:

            e1 = threading.Event()

            def process_tick(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 8)
                e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener_column(event):
                batch = event['data']
                self.assertEqual(len(batch), 8)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 20)
                e2.set()

            listener.process_batch += process_batch_listener_column

            e3 = threading.Event()

            def process_minibatch_listener_column(event):
                batch = event['data']
                self.assertEqual(len(batch), 8)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 4)
                e3.set()

            listener.process_minibatch += process_minibatch_listener_column

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 8)

                for v in d.values():
                    self.assertEqual(len(v), 4)

                self.assertNotEqual(d['Date'][0], d['Date'][1])

                if i == 1:
                    break

            e2.wait()
            e3.wait()

    def test_daily_row_mode(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsDailyFilter(ticker="IBM", num_days=20)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, column_mode=False) as listener, listener.minibatch_provider() as provider:
            e1 = threading.Event()

            def process_tick(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 8)
                e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener(event):
                batch = event['data']
                self.assertEqual(len(batch[0]), 8)
                self.assertEqual(len(batch), 20)
                e2.set()

            listener.process_batch += process_batch_listener

            e3 = threading.Event()

            def process_minibatch_listener(event):
                batch = event['data']
                self.assertEqual(len(batch[0]), 8)
                self.assertEqual(len(batch), 4)
                e3.set()

            listener.process_minibatch += process_minibatch_listener

            e2.wait()
            e3.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 4)

                for item in d:
                    self.assertEqual(len(item), 8)

                self.assertNotEqual(d[0]['Date'], d[1]['Date'])

                if i == 1:
                    break

if __name__ == '__main__':
    unittest.main()
