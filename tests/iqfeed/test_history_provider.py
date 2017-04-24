import unittest
import shutil


from atpy.data.iqfeed.iqfeed_history_provider import *


class TestIQFeedHistory(unittest.TestCase):
    """
    IQFeed history provider test, which checks whether the class works in basic terms
    """

    def test_ticks(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker="IBM", max_ticks=20)

        try:
            with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider, lmdb_path='/tmp/test_history_provider_test_ticks_column_mode') as listener, listener.minibatch_provider() as provider:

                e1 = threading.Event()

                def process_tick(event):
                    try:
                        data = event['data']
                        self.assertEqual(len(list(data.keys())), 14)
                    finally:
                        e1.set()

                listener.process_datum += process_tick

                e2 = threading.Event()

                def process_batch_listener_column(event):
                    try:
                        batch = event['data']
                        self.assertEqual(batch.shape, (20, 14))
                    finally:
                        e2.set()

                listener.process_batch += process_batch_listener_column

                e3 = threading.Event()

                def process_minibatch_listener_column(event):
                    try:
                        batch = event['data']
                        self.assertEqual(batch.shape, (4, 14))
                    finally:
                        e3.set()

                listener.process_minibatch += process_minibatch_listener_column

                for i, d in enumerate(provider):
                    self.assertEqual(d.shape, (4, 14))

                    self.assertNotEqual(d['TickID'].iloc[0], d['TickID'].iloc[1])

                    if i == 1:
                        break

                e2.wait()
                e3.wait()
        finally:
            shutil.rmtree('/tmp/test_history_provider_test_ticks_column_mode')

    def test_multiple_ticks(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker=["IBM", "AAPL"], max_ticks=100)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider) as listener, listener.minibatch_provider() as provider:

            e1 = threading.Event()

            def process_tick(event):
                try:
                    data = event['data']
                    self.assertEqual(len(list(data.keys())), 2)
                    self.assertEqual(len(list(data['IBM'].keys())), 14)
                    self.assertEqual(len(list(data['AAPL'].keys())), 14)
                finally:
                    e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener_column(event):
                try:
                    batch = event['data']
                    self.assertEqual(batch['IBM'].shape[1], 14)
                    self.assertEqual(batch['AAPL'].shape[1], 14)
                finally:
                    e2.set()

            listener.process_batch += process_batch_listener_column

            e3 = threading.Event()

            def process_minibatch_listener_column(event):
                try:
                    batch = event['data']
                    self.assertEqual(batch['IBM'].shape, (4, 14))
                    self.assertEqual(batch['AAPL'].shape, (4, 14))
                finally:
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

    def test_bar(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=20)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider) as listener, listener.minibatch_provider() as provider:

            e1 = threading.Event()

            def process_bar(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 9)
                e1.set()

            listener.process_datum += process_bar

            e2 = threading.Event()

            def process_batch_listener_column(event):
                batch = event['data']
                self.assertEqual(batch.shape, (20, 9))
                e2.set()

            listener.process_batch += process_batch_listener_column

            e3 = threading.Event()

            def process_minibatch_listener_column(event):
                batch = event['data']
                self.assertEqual(batch.shape, (4, 9))
                e3.set()

            listener.process_minibatch += process_minibatch_listener_column

            for i, d in enumerate(provider):
                self.assertEqual(d.shape, (4, 9))
                self.assertNotEqual(d['Time Stamp'].iloc[0], d['Time Stamp'].iloc[1])

                if i == 1:
                    break

            e2.wait()
            e3.wait()

    def test_bars(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsDailyFilter(ticker=["IBM", "AAPL"], num_days=20)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider) as listener, listener.minibatch_provider() as provider:

            e1 = threading.Event()

            def process_bar(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 2)
                self.assertEqual(len(list(data['IBM'].keys())), 8)
                self.assertEqual(len(list(data['AAPL'].keys())), 8)
                e1.set()

            listener.process_datum += process_bar

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

        try:
            with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, num_connections=2, filter_provider=filter_provider, lmdb_path='/tmp/test_history_provider_test_bars_row_mode') as listener, listener.minibatch_provider() as provider:

                e1 = threading.Event()

                def process_bar(event):
                    data = event['data']
                    self.assertEqual(len(list(data.keys())), 2)
                    self.assertEqual(len(list(data['IBM'].keys())), 9)
                    self.assertEqual(len(list(data['AAPL'].keys())), 9)
                    e1.set()

                listener.process_datum += process_bar

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
        finally:
            shutil.rmtree('/tmp/test_history_provider_test_bars_row_mode')

    def test_bar_adjust(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsInPeriodFilter(ticker="PLUS", bgn_prd=datetime.datetime(2017, 3, 31), end_prd=datetime.datetime(2017, 4, 5), interval_len=3600, ascend=True, interval_type='s', max_ticks=100)

        try:
            with IQFeedHistoryListener(fire_batches=True, fire_ticks=True, filter_provider=filter_provider, lmdb_path='/tmp/test_history_provider_test_bars_row_mode') as listener, listener.batch_provider() as provider:
                e1 = threading.Event()

                def process_bar(event):
                    try:
                        self.assertLess(event['data']['Open'], 68)
                        self.assertGreater(event['data']['Open'], 65)
                    finally:
                        e1.set()

                listener.process_datum += process_bar

                e1.wait()

                for i, d in enumerate(provider):
                    self.assertLess(d['Open'].max(), 68)
                    self.assertGreater(d['Open'].min(), 65)

                    if i == 1:
                        break
        finally:
            shutil.rmtree('/tmp/test_history_provider_test_bars_row_mode')

    def test_daily(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsDailyFilter(ticker="IBM", num_days=20)

        with IQFeedHistoryListener(minibatch=4, fire_batches=True, fire_ticks=True, filter_provider=filter_provider) as listener, listener.minibatch_provider() as provider:

            e1 = threading.Event()

            def process_tick(event):
                data = event['data']
                self.assertEqual(len(list(data.keys())), 8)
                e1.set()

            listener.process_datum += process_tick

            e2 = threading.Event()

            def process_batch_listener_column(event):
                batch = event['data']
                self.assertEqual(batch.shape, (20, 8))
                e2.set()

            listener.process_batch += process_batch_listener_column

            e3 = threading.Event()

            def process_minibatch_listener_column(event):
                batch = event['data']
                self.assertEqual(batch.shape, (4, 8))
                e3.set()

            listener.process_minibatch += process_minibatch_listener_column

            for i, d in enumerate(provider):
                self.assertEqual(d.shape, (4, 8))
                self.assertNotEqual(d['Date'].iloc[0], d['Date'].iloc[1])

                if i == 1:
                    break

            e2.wait()
            e3.wait()

    def test_continuous_bars(self):
        now = datetime.datetime.now()

        filter_provider = BarsInPeriodProvider(ticker=['AAPL', 'GOOG'], bgn_prd=datetime.datetime(now.year, 1, 1), delta=datetime.timedelta(days=1), interval_len=3600, ascend=True, interval_type='s')

        try:
            with IQFeedHistoryListener(fire_batches=True, fire_ticks=True, minibatch=10, filter_provider=filter_provider, lmdb_path='/tmp/test_continuous_bars') as listener, listener.batch_provider() as provider:
                events_count = {'bars': 0, 'batches': 0, 'minibatches': 0}

                e1 = threading.Event()

                def process_bar(event):
                    try:
                        self.assertTrue(len(event['data']) > 0)
                    finally:
                        events_count['bars'] += 1
                        if events_count['bars'] >= 2:
                            e1.set()

                listener.process_datum += process_bar

                e2 = threading.Event()

                def process_batch_listener(event):
                    try:
                        self.assertTrue(len(event['data']) > 0)
                        self.assertEqual(len(event['data'].shape), 3)
                    finally:
                        events_count['batches'] += 1
                        if events_count['batches'] >= 2:
                            e2.set()

                listener.process_batch += process_batch_listener

                e3 = threading.Event()

                def process_minibatch_listener(event):
                    try:
                        self.assertTrue(len(event['data']) > 0)
                        self.assertEqual(event['data'].shape[1:], (10, 9))
                    finally:
                        events_count['minibatches'] += 1
                        if events_count['minibatches'] >= 2:
                            e3.set()

                listener.process_minibatch += process_minibatch_listener

                e1.wait()
                e2.wait()
                e3.wait()
        finally:
            shutil.rmtree('/tmp/test_continuous_bars')

    def test_continuous_ticks(self):
        now = datetime.datetime.now()

        filter_provider = TicksInPeriodProvider(ticker=['AAPL', 'GOOG'], bgn_prd=datetime.datetime(now.year, 1, 3), delta=datetime.timedelta(hours=1))

        try:
            with IQFeedHistoryListener(fire_batches=True, fire_ticks=True, minibatch=10, filter_provider=filter_provider, lmdb_path='/tmp/test_continuous_ticks') as listener, listener.batch_provider() as provider:
                events_count = {'ticks': 0, 'batches': 0, 'minibatches': 0}

                e1 = threading.Event()

                def process_tick(event):
                    try:
                        self.assertTrue(len(event['data']) > 0)
                    finally:
                        events_count['ticks'] += 1
                        if events_count['ticks'] >= 2:
                            e1.set()

                listener.process_datum += process_tick

                e2 = threading.Event()

                def process_batch_listener(event):
                    try:
                        self.assertTrue(len(event['data']) > 0)
                        self.assertEqual(len(event['data'].shape), 3)
                    finally:
                        events_count['batches'] += 1
                        if events_count['batches'] >= 2:
                            e2.set()

                listener.process_batch += process_batch_listener

                e3 = threading.Event()

                def process_minibatch_listener(event):
                    try:
                        self.assertTrue(len(event['data']) > 0)
                        self.assertEqual(event['data'].shape[1:], (10, 14))
                    finally:
                        events_count['minibatches'] += 1
                        if events_count['minibatches'] >= 2:
                            e3.set()

                listener.process_minibatch += process_minibatch_listener

                e1.wait()
                e2.wait()
                e3.wait()
        finally:
            shutil.rmtree('/tmp/test_continuous_ticks')

    def test_bars_performance(self):
        now = datetime.datetime.now()

        filter_provider = BarsInPeriodProvider(
            ticker=['MMM', 'AXP', 'AAPL', 'BA', 'CAT', 'CVX', 'CSCO', 'KO', 'DO', 'XOM', 'GE', 'GS', 'HD', 'IBM', 'INTC', 'JNJ', 'JPM', 'MCD', 'MRK', 'MSFT', 'NKE', 'PFE', 'PG', 'TRV', 'UNH', 'UTX', 'VZ', 'V', 'WMT', 'DIS'],
            bgn_prd=datetime.datetime(now.year - 1, 1, 1), delta=datetime.timedelta(days=100), interval_len=300, ascend=True, interval_type='s')

        with IQFeedHistoryListener(fire_batches=True, fire_ticks=False, minibatch=128, num_connections=10, filter_provider=filter_provider, run_async=False) as listener:
            def process_batch_listener(event):
                pass

            listener.process_batch += process_batch_listener

            def process_minibatch_listener(event):
                pass

            listener.process_minibatch += process_minibatch_listener

            listener.produce()

            self.assertLess(datetime.datetime.now() - now, datetime.timedelta(seconds=90))


if __name__ == '__main__':
    unittest.main()
