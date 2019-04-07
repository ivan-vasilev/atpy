import unittest

from atpy.data.iqfeed.iqfeed_history_provider import *
from atpy.data.util import resample_bars
from pyevents.events import AsyncListeners, SyncListeners


class TestIQFeedHistory(unittest.TestCase):
    """
    IQFeed history provider test, which checks whether the class works in basic terms
    """

    def test_ticks(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker="IBM", max_ticks=20)

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, num_connections=2) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_batch_listener_column(event):
                if event['type'] == 'level_1_tick_batch':
                    try:
                        batch = event['data']
                        self.assertEqual(batch.shape, (20, 14))
                    finally:
                        e1.set()

            listeners += process_batch_listener_column

            listener.start()

            e1.wait()

            for i, d in enumerate(provider):
                self.assertEqual(d.shape, (20, 14))

                self.assertNotEqual(d['tick_id'].iloc[0], d['tick_id'].iloc[1])

                if i == 1:
                    break

    def test_multiple_ticks(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker=["IBM", "AAPL"], max_ticks=100)

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, num_connections=2) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_batch_listener_column(event):
                if event['type'] == 'level_1_tick_batch':
                    try:
                        batch = event['data']
                        self.assertEqual(batch.shape[1], 14)
                    finally:
                        e1.set()

            listeners += process_batch_listener_column

            listener.start()

            e1.wait()

            for i, d in enumerate(provider):
                self.assertEqual(d.shape[1], 14)

                if i == 1:
                    break

    def test_bar(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=20)

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, num_connections=2) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_batch_listener_column(event):
                if event['type'] == 'bar_batch':
                    batch = event['data']
                    self.assertEqual(batch.shape, (20, 9))
                    e1.set()

            listeners += process_batch_listener_column

            listener.start()

            e1.wait()

            for i, d in enumerate(provider):
                self.assertEqual(d.shape, (20, 9))
                self.assertNotEqual(d['timestamp'].iloc[0], d['timestamp'].iloc[1])

                if i == 1:
                    break

    def test_tick_bar(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsFilter(ticker="IBM", interval_len=60, interval_type='t', max_bars=20)

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, num_connections=2) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_batch_listener_column(event):
                if event['type'] == 'bar_batch':
                    batch = event['data']
                    self.assertEqual(batch.shape, (20, 9))
                    e1.set()

            listeners += process_batch_listener_column

            listener.start()

            e1.wait()

            for i, d in enumerate(provider):
                self.assertEqual(d.shape, (20, 9))
                self.assertNotEqual(d['timestamp'].iloc[0], d['timestamp'].iloc[1])

                if i == 1:
                    break

    def test_volume_bar(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsFilter(ticker="IBM", interval_len=100000, interval_type='v', max_bars=20)

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, num_connections=2) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_batch_listener_column(event):
                if event['type'] == 'bar_batch':
                    batch = event['data']
                    self.assertEqual(batch.shape, (20, 9))
                    e1.set()

            listeners += process_batch_listener_column

            listener.start()

            e1.wait()

            for i, d in enumerate(provider):
                self.assertEqual(d.shape, (20, 9))
                self.assertNotEqual(d['timestamp'].iloc[0], d['timestamp'].iloc[1])

                if i == 1:
                    break

    def test_bars(self):
        batch_len = 20
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsFilter(ticker=["IBM", "AAPL", "GOOG"], interval_len=60, interval_type='s', max_bars=batch_len)

        listeners = SyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, num_connections=2) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_batch_listener_column(event):
                if event['type'] == 'bar_batch':
                    batch = event['data']
                    batch.index.get_level_values('timestamp')
                    self.assertEqual(len(batch.index.levels), 2)
                    self.assertEqual(len(batch.index.levels[0]), 3)

                    self.assertGreaterEqual(len(batch.index.levels[1]), batch_len)

                    self.assertEqual(batch.loc['AAPL'].shape[1], 9)
                    self.assertEqual(batch.loc['IBM'].shape[1], 9)

                    self.assertGreaterEqual(len(batch.loc['AAPL'][batch.loc['IBM'].columns[0]]), batch_len)

                    self.assertFalse(batch.isnull().values.any())

                    e1.set()

            listeners += process_batch_listener_column

            listener.start()

            e1.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d.index.levels), 2)
                self.assertEqual(len(d.index.levels[0]), 3)

                self.assertGreaterEqual(len(d.index.levels[1]), batch_len)

                self.assertEqual(d.loc['AAPL'].shape[1], 9)
                self.assertEqual(d.loc['IBM'].shape[1], 9)

                self.assertGreaterEqual(len(d.loc['AAPL'][d.loc['IBM'].columns[0]]), batch_len)

                self.assertFalse(d.isnull().values.any())

                if i == 1:
                    break

    def test_bars_2(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsInPeriodFilter(ticker=["IBM", "AAPL"], bgn_prd=datetime.datetime(2017, 3, 1), end_prd=datetime.datetime(2017, 3, 2), interval_len=60, ascend=True, interval_type='s', max_ticks=20)

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, num_connections=2, filter_provider=filter_provider) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_batch_listener_column(event):
                if event['type'] == 'bar_batch':
                    batch = event['data']
                    self.assertEqual(len(batch.index.levels), 2)
                    self.assertEqual(batch.loc['AAPL'].shape[1], 9)
                    self.assertEqual(batch.loc['IBM'].shape[1], 9)
                    self.assertFalse(batch.isnull().values.any())
                    e1.set()

            listeners += process_batch_listener_column

            listener.start()

            e1.wait()

            for i, d in enumerate(provider):
                self.assertEqual(len(d.index.levels), 2)
                self.assertEqual(d.loc['AAPL'].shape[1], 9)
                self.assertEqual(d.loc['IBM'].shape[1], 9)
                self.assertFalse(d.isnull().values.any())

                if i == 1:
                    break

    def test_resample(self):
        with IQFeedHistoryProvider() as provider:
            df = provider.request_data(BarsFilter(ticker=["IBM", "AAPL", "GOOG"], interval_len=60, interval_type='s', max_bars=100), sync_timestamps=False)
            resampled_df = resample_bars(df, '5min')
            self.assertLess(len(resampled_df), len(df))
            self.assertEqual(df['volume'].sum(), resampled_df['volume'].sum())

    def test_daily(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsDailyFilter(ticker="IBM", num_days=20)

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, num_connections=2) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_batch_listener_column(event):
                if event['type'] == 'daily_batch':
                    batch = event['data']
                    self.assertEqual(batch.shape, (20, 8))
                    e1.set()

            listeners += process_batch_listener_column
            listener.start()
            e1.wait()

            for i, d in enumerate(provider):
                self.assertEqual(d.shape, (20, 8))

                if i == 1:
                    break

    def test_daily_2(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsDailyFilter(ticker=["IBM", "AAPL"], num_days=20)

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, num_connections=2) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_batch_listener_column(event):
                if event['type'] == 'daily_batch':
                    batch = event['data']
                    self.assertEqual(batch.shape, (40, 8))
                    e1.set()

            listeners += process_batch_listener_column

            listener.start()

            e1.wait()

            for i, d in enumerate(provider):
                self.assertEqual(d.shape, (40, 8))

                if i == 1:
                    break

    def test_continuous_bars(self):
        now = datetime.datetime.now()

        filter_provider = BarsInPeriodProvider(ticker=['AAPL', 'GOOG'], bgn_prd=datetime.datetime(now.year - 2, 1, 1), delta=relativedelta(days=10), interval_len=3600, ascend=True, interval_type='s')

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, num_connections=2) as listener:
            events_count = {'bars': 0, 'batches': 0, 'minibatches': 0}

            e1 = threading.Event()

            def process_batch_listener(event):
                if event['type'] == 'bar_batch':
                    try:
                        self.assertEqual(len(event['data'].index.levels[0]), 2)
                        self.assertFalse(event['data'].isnull().values.any())
                    finally:
                        events_count['batches'] += 1
                        if events_count['batches'] >= 2:
                            e1.set()

            listeners += process_batch_listener
            listener.start()
            e1.wait()

    def test_continuous_ticks(self):
        filter_provider = TicksInPeriodProvider(ticker=['AAPL', 'GOOG'], bgn_prd=datetime.datetime.now() - datetime.timedelta(days=1), ascend=True, delta=relativedelta(minutes=15))

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, num_connections=2) as listener:
            events_count = {'ticks': 0, 'batches': 0, 'minibatches': 0}

            e1 = threading.Event()

            def process_batch_listener(event):
                if event['type'] == 'level_1_tick_batch':
                    try:
                        self.assertTrue(len(event['data']) > 0)
                        self.assertEqual(len(event['data'].shape), 2)
                        self.assertEqual(event['data'].shape[1], 14)
                    finally:
                        events_count['batches'] += 1
                        if events_count['batches'] >= 2:
                            e1.set()

            listeners += process_batch_listener
            listener.start()
            e1.wait()

    # TODO
    # def test_bars_performance(self):
    #     now = datetime.datetime.now()
    #     filter_provider = BarsInPeriodProvider(
    #         ticker=['MMM', 'AXP', 'AAPL', 'BA', 'CAT', 'CVX', 'CSCO', 'KO', 'DO', 'XOM', 'GE', 'GS', 'HD', 'IBM', 'INTC', 'JNJ', 'JPM', 'MCD', 'MRK', 'MSFT', 'NKE', 'PFE', 'PG', 'TRV', 'UNH', 'UTX', 'VZ', 'V', 'WMT', 'DIS'],
    #         bgn_prd=datetime.datetime.combine(datetime.date(now.year - 1, month=now.month, day=now.day), datetime.datetime.min.time()), delta=relativedelta(days=100), interval_len=300, ascend=True, interval_type='s')
    #
    #     listeners = AsyncListeners()
    #
    #     with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, num_connections=10, filter_provider=filter_provider, run_async=False) as listener:
    #         e1 = threading.Event()
    #
    #         def process_batch_listener(event):
    #             if event['type'] == 'bar_batch':
    #                 self.assertFalse(event['data'].isnull().values.any())
    #                 e1.set()
    #
    #         listeners += process_batch_listener
    #
    #         now = datetime.datetime.now()
    #
    #         listener.start()
    #
    #         e1.wait()
    #
    #         self.assertLess(datetime.datetime.now() - now, datetime.timedelta(seconds=40))

    def test_synchronize_timestamps(self):
        with IQFeedHistoryProvider(num_connections=2) as history:
            end_prd = datetime.datetime(2017, 5, 1)

            # test multi-symbol request
            requested_data = history.request_data(BarsInPeriodFilter(ticker=["AAPL", "IBM"], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'), sync_timestamps=False)
            self.assertNotEqual(requested_data.loc['AAPL'].shape, requested_data.loc['IBM'].shape)

            requested_data = history.request_data(BarsInPeriodFilter(ticker=["AAPL", "IBM"], bgn_prd=datetime.datetime(2017, 4, 1), end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'), sync_timestamps=True)
            self.assertEqual(requested_data.loc['AAPL'].shape, requested_data.loc['IBM'].shape)


if __name__ == '__main__':
    unittest.main()
