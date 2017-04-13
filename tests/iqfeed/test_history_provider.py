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
                    self.assertEqual(len(list(data.keys())), 14)
                    e1.set()

                listener.process_datum += process_tick

                e2 = threading.Event()

                def process_batch_listener_column(event):
                    batch = event['data']
                    self.assertEqual(len(batch), 14)
                    self.assertEqual(len(batch[list(batch.keys())[0]]), 20)
                    e2.set()

                listener.process_batch += process_batch_listener_column

                e3 = threading.Event()

                def process_minibatch_listener_column(event):
                    batch = event['data']
                    self.assertEqual(len(batch), 14)
                    self.assertEqual(len(batch[list(batch.keys())[0]]), 4)
                    e3.set()

                listener.process_minibatch += process_minibatch_listener_column

                for i, d in enumerate(provider):
                    self.assertEqual(len(d), 14)

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
                self.assertEqual(len(list(data.keys())), 14)
                e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener(event):
                batch = event['data']
                self.assertEqual(len(batch[0]), 14)
                self.assertEqual(len(batch), 20)
                e2.set()

            listener.process_batch += process_batch_listener

            e3 = threading.Event()

            def process_minibatch_listener(event):
                batch = event['data']
                self.assertEqual(len(batch[0]), 14)
                self.assertEqual(len(batch), 4)
                e3.set()

            listener.process_minibatch += process_minibatch_listener

            e2.wait()
            e3.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 4)

                for item in d:
                    self.assertEqual(len(item), 14)

                self.assertNotEqual(d[0]['TickID'], d[1]['TickID'])

                if i == 1:
                    break

    def test_multiple_ticks(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker=["IBM", "AAPL"], max_ticks=100)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, column_mode=True) as listener, listener.minibatch_provider() as provider:

            e1 = threading.Event()

            def process_tick(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 2)
                self.assertEqual(len(list(data['IBM'].keys())), 14)
                self.assertEqual(len(list(data['AAPL'].keys())), 14)
                e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener_column(event):
                batch = event['data']
                self.assertEqual(batch['IBM'].shape[1], 14)
                self.assertEqual(batch['AAPL'].shape[1], 14)
                e2.set()

            listener.process_batch += process_batch_listener_column

            e3 = threading.Event()

            def process_minibatch_listener_column(event):
                batch = event['data']
                self.assertEqual(batch['IBM'].shape, (4, 14))
                self.assertEqual(batch['AAPL'].shape, (4, 14))
                e3.set()

            listener.process_minibatch += process_minibatch_listener_column

            for i, d in enumerate(provider):
                self.assertEqual(d['IBM'].shape, (4, 14))
                self.assertEqual(d['AAPL'].shape, (4, 14))

                self.assertEqual(d['IBM']['Time Stamp'].iloc[0], d['AAPL']['Time Stamp'].iloc[0])
                self.assertNotEqual(d['IBM']['Time Stamp'].iloc[0], d['AAPL']['Time Stamp'].iloc[1])

                if i == 1:
                    break

            e2.wait()
            e3.wait()

    def test_bar_column_mode(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=20)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, column_mode=True) as listener, listener.minibatch_provider() as provider:

            e1 = threading.Event()

            def process_tick(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 9)
                e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener_column(event):
                batch = event['data']
                self.assertEqual(len(batch), 9)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 20)
                e2.set()

            listener.process_batch += process_batch_listener_column

            e3 = threading.Event()

            def process_minibatch_listener_column(event):
                batch = event['data']
                self.assertEqual(len(batch), 9)
                self.assertEqual(len(batch[list(batch.keys())[0]]), 4)
                e3.set()

            listener.process_minibatch += process_minibatch_listener_column

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 9)

                for v in d.values():
                    self.assertEqual(len(v), 4)

                self.assertNotEqual(d['Time Stamp'][0], d['Time Stamp'][1])

                if i == 1:
                    break

            e2.wait()
            e3.wait()

    def test_bars(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsDailyFilter(ticker=["IBM", "AAPL"], num_days=20)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, column_mode=True) as listener, listener.minibatch_provider() as provider:

            e1 = threading.Event()

            def process_tick(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 2)
                self.assertEqual(len(list(data['IBM'].keys())), 8)
                self.assertEqual(len(list(data['AAPL'].keys())), 8)
                e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener_column(event):
                batch = event['data']
                self.assertEqual(len(batch), 2)
                self.assertEqual(batch['AAPL'].shape, (20, 8))
                self.assertEqual(batch['IBM'].shape, (20, 8))
                self.assertEqual(len(list(batch['AAPL'][batch['IBM'].columns[0]])), 20)
                e2.set()

            listener.process_batch += process_batch_listener_column

            e3 = threading.Event()

            def process_minibatch_listener_column(event):
                batch = event['data']
                self.assertEqual(len(batch), 2)
                self.assertEqual(batch['AAPL'].shape, (4, 8))
                self.assertEqual(batch['IBM'].shape, (4, 8))
                self.assertEqual(len(list(batch['AAPL'][batch['IBM'].columns[0]])), 4)
                e3.set()

            listener.process_minibatch += process_minibatch_listener_column

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 2)
                self.assertEqual(d['AAPL'].shape, (4, 8))
                self.assertEqual(d['IBM'].shape, (4, 8))
                self.assertEqual(len(list(d['AAPL'][d['IBM'].columns[0]])), 4)

                self.assertEqual(d['IBM']['Date'].iloc[0], d['AAPL']['Date'].iloc[0])
                self.assertNotEqual(d['IBM']['Date'].iloc[0], d['AAPL']['Date'].iloc[1])

                if i == 1:
                    break

            e2.wait()
            e3.wait()

    def test_bars_2(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsInPeriodFilter(ticker=["IBM", "AAPL"], bgn_prd=datetime.datetime(2017, 3, 1), end_prd=datetime.datetime(2017, 3, 2), interval_len=60, ascend=True, interval_type='s', max_ticks=20)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, column_mode=True) as listener, listener.minibatch_provider() as provider:

            e1 = threading.Event()

            def process_tick(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 2)
                self.assertEqual(len(list(data['IBM'].keys())), 9)
                self.assertEqual(len(list(data['AAPL'].keys())), 9)
                e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener_column(event):
                batch = event['data']
                self.assertEqual(len(batch), 2)
                self.assertEqual(batch['AAPL'].shape[1], 9)
                self.assertEqual(batch['IBM'].shape[1], 9)
                e2.set()

            listener.process_batch += process_batch_listener_column

            e3 = threading.Event()

            def process_minibatch_listener_column(event):
                batch = event['data']
                self.assertEqual(len(batch), 2)
                self.assertEqual(batch['AAPL'].shape, (4, 9))
                self.assertEqual(batch['IBM'].shape, (4, 9))
                self.assertEqual(len(list(batch['AAPL'][batch['IBM'].columns[0]])), 4)
                e3.set()

            listener.process_minibatch += process_minibatch_listener_column

            for i, d in enumerate(provider):
                self.assertEqual(len(d), 2)
                self.assertEqual(d['AAPL'].shape, (4, 9))
                self.assertEqual(d['IBM'].shape, (4, 9))
                self.assertEqual(len(list(d['AAPL'][d['IBM'].columns[0]])), 4)

                self.assertEqual(d['IBM']['Time Stamp'].iloc[0], d['AAPL']['Time Stamp'].iloc[0])
                self.assertNotEqual(d['IBM']['Time Stamp'].iloc[0], d['AAPL']['Time Stamp'].iloc[1])

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
                    self.assertEqual(len(list(data.keys())), 9)
                    e1.set()

                listener.process_datum += process_tick

                e2 = threading.Event()

                def process_batch_listener(event):
                    batch = event['data']
                    self.assertEqual(len(batch[0]), 9)
                    self.assertEqual(len(batch), 20)
                    e2.set()

                listener.process_batch += process_batch_listener

                e3 = threading.Event()

                def process_minibatch_listener(event):
                    batch = event['data']
                    self.assertEqual(len(batch[0]), 9)
                    self.assertEqual(len(batch), 4)
                    e3.set()

                listener.process_minibatch += process_minibatch_listener

                e2.wait()
                e3.wait()

                for i, d in enumerate(provider):
                    self.assertEqual(len(d), 4)

                    for item in d:
                        self.assertEqual(len(item), 9)

                    self.assertNotEqual(d[0]['Time Stamp'], d[1]['Time Stamp'])

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
