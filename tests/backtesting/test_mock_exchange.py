import unittest

from atpy.backtesting.data_replay import DataReplay, DataReplayEvents
from atpy.backtesting.mock_exchange import MockExchange, StaticSlippageLoss, PerShareCommissionLoss
from atpy.data.iqfeed.iqfeed_bar_data_provider import *
from atpy.data.iqfeed.iqfeed_history_provider import *
from atpy.data.iqfeed.iqfeed_level_1_provider import *
from atpy.portfolio.order import *
from pyevents.events import *


class TestMockExchange(unittest.TestCase):
    """
    Test Mock Orders
    """

    def setUp(self):
        logging.basicConfig(level=logging.INFO)

    def test_market_order(self):
        listeners = AsyncListeners()

        with IQFeedLevel1Listener(listeners=listeners) as level_1:
            order_request_events = SyncListeners()

            me = MockExchange(listeners=listeners,
                              order_requests_event_stream=order_request_events,
                              tick_event_stream=level_1.tick_event_filter(),
                              order_processor=StaticSlippageLoss(0.1),
                              commission_loss=PerShareCommissionLoss(0.1))

            fulfilled_orders = me.fulfilled_orders_stream()

            e1 = threading.Event()
            fulfilled_orders += lambda x: e1.set() if x.symbol == 'GOOG' else None

            e2 = threading.Event()
            fulfilled_orders += lambda x: e2.set() if x.symbol == 'AAPL' else None

            e3 = threading.Event()
            fulfilled_orders += lambda x: e3.set() if x.symbol == 'IBM' else None

            level_1.watch('GOOG')
            level_1.watch('AAPL')
            level_1.watch('IBM')

            o1 = MarketOrder(Type.BUY, 'GOOG', 1)
            order_request_events(o1)

            o2 = MarketOrder(Type.BUY, 'AAPL', 3)
            order_request_events(o2)

            o3 = MarketOrder(Type.SELL, 'IBM', 1)
            order_request_events(o3)

            e1.wait()
            e2.wait()
            e3.wait()

        self.assertEqual(o1.obtained_quantity, 1)
        self.assertGreater(o1.cost, 0)
        self.assertIsNotNone(o1.fulfill_time)

        self.assertEqual(o2.obtained_quantity, 3)
        self.assertGreater(o2.cost, 0)
        self.assertIsNotNone(o2.fulfill_time)

        self.assertEqual(o3.obtained_quantity, 1)
        self.assertGreater(o3.cost, 0)
        self.assertIsNotNone(o3.fulfill_time)

    def test_bar_market_order(self):
        listeners = AsyncListeners()

        with IQFeedBarDataListener(interval_len=300, mkt_snapshot_depth=10, listeners=listeners) as bars:
            order_request_events = SyncListeners()

            me = MockExchange(listeners=listeners,
                              order_requests_event_stream=order_request_events,
                              bar_event_stream=bars.bar_event_stream(),
                              order_processor=StaticSlippageLoss(0.1),
                              commission_loss=PerShareCommissionLoss(0.1))

            fulfilled_orders = me.fulfilled_orders_stream()

            e1 = threading.Event()
            fulfilled_orders += lambda x: e1.set() if x.symbol == 'GOOG' else None

            e2 = threading.Event()
            fulfilled_orders += lambda x: e2.set() if x.symbol == 'AAPL' else None

            e3 = threading.Event()
            fulfilled_orders += lambda x: e3.set() if x.symbol == 'IBM' else None

            bars.watch_bars('GOOG')
            bars.watch_bars('AAPL')
            bars.watch_bars('IBM')

            o1 = MarketOrder(Type.BUY, 'GOOG', 1)
            order_request_events(o1)

            o2 = MarketOrder(Type.BUY, 'AAPL', 3)
            order_request_events(o2)

            o3 = MarketOrder(Type.SELL, 'IBM', 1)
            order_request_events(o3)

            e1.wait()
            e2.wait()
            e3.wait()

        self.assertEqual(o1.obtained_quantity, 1)
        self.assertGreater(o1.cost, 0)
        self.assertIsNotNone(o1.fulfill_time)

        self.assertEqual(o2.obtained_quantity, 3)
        self.assertGreater(o2.cost, 0)
        self.assertIsNotNone(o2.fulfill_time)

        self.assertEqual(o3.obtained_quantity, 1)
        self.assertGreater(o3.cost, 0)
        self.assertIsNotNone(o3.fulfill_time)

    def test_historical_bar_market_order(self):
        listeners = AsyncListeners()

        with IQFeedHistoryProvider() as provider:
            f = BarsFilter(ticker=["GOOG", "AAPL", "IBM"], interval_len=60, interval_type='s', max_bars=20)
            data = provider.request_data(f, sync_timestamps=False).swaplevel(0, 1).sort_index()
            dre = DataReplayEvents(listeners=listeners,
                                   data_replay=DataReplay().add_source([data], 'data', historical_depth=5),
                                   event_name='bar')

            order_request_events = SyncListeners()

            me = MockExchange(listeners=listeners,
                              order_requests_event_stream=order_request_events,
                              bar_event_stream=dre.event_filter_by_source('data'),
                              order_processor=StaticSlippageLoss(0.1),
                              commission_loss=PerShareCommissionLoss(0.1))

            fulfilled_orders = me.fulfilled_orders_stream()

            e1 = threading.Event()
            fulfilled_orders += lambda x: e1.set() if x.symbol == 'GOOG' else None

            e2 = threading.Event()
            fulfilled_orders += lambda x: e2.set() if x.symbol == 'AAPL' else None

            e3 = threading.Event()
            fulfilled_orders += lambda x: e3.set() if x.symbol == 'IBM' else None

            o1 = MarketOrder(Type.BUY, 'GOOG', 1)
            order_request_events(o1)

            o2 = MarketOrder(Type.BUY, 'AAPL', 3)
            order_request_events(o2)

            o3 = MarketOrder(Type.SELL, 'IBM', 1)
            order_request_events(o3)

            dre.start()

            e1.wait()
            e2.wait()
            e3.wait()

        self.assertEqual(o1.obtained_quantity, 1)
        self.assertGreater(o1.cost, 0)
        self.assertIsNotNone(o1.fulfill_time)

        self.assertEqual(o2.obtained_quantity, 3)
        self.assertGreater(o2.cost, 0)
        self.assertIsNotNone(o2.fulfill_time)

        self.assertEqual(o3.obtained_quantity, 1)
        self.assertGreater(o3.cost, 0)
        self.assertIsNotNone(o3.fulfill_time)

    def test_limit_order(self):
        listeners = AsyncListeners()

        with IQFeedLevel1Listener(listeners=listeners) as level_1:
            order_request_events = SyncListeners()

            me = MockExchange(listeners=listeners,
                              order_requests_event_stream=order_request_events,
                              tick_event_stream=level_1.tick_event_filter(),
                              order_processor=StaticSlippageLoss(0.1),
                              commission_loss=PerShareCommissionLoss(0.1))

            fulfilled_orders = me.fulfilled_orders_stream()

            e1 = threading.Event()
            fulfilled_orders += lambda x: e1.set() if x.symbol == 'GOOG' else None

            e2 = threading.Event()
            fulfilled_orders += lambda x: e2.set() if x.symbol == 'AAPL' else None

            e3 = threading.Event()
            fulfilled_orders += lambda x: e3.set() if x.symbol == 'IBM' else None

            level_1.watch('GOOG')
            level_1.watch('AAPL')
            level_1.watch('IBM')

            o1 = LimitOrder(Type.BUY, 'GOOG', 1, 99999)
            order_request_events(o1)

            o2 = LimitOrder(Type.BUY, 'AAPL', 3, 99999)
            order_request_events(o2)

            o3 = LimitOrder(Type.SELL, 'IBM', 1, 0)
            order_request_events(o3)

            e1.wait()
            e2.wait()
            e3.wait()

        self.assertEqual(o1.obtained_quantity, 1)
        self.assertGreater(o1.cost, 0)
        self.assertIsNotNone(o1.fulfill_time)

        self.assertEqual(o2.obtained_quantity, 3)
        self.assertGreater(o2.cost, 0)
        self.assertIsNotNone(o2.fulfill_time)

        self.assertEqual(o3.obtained_quantity, 1)
        self.assertGreater(o3.cost, 0)
        self.assertIsNotNone(o3.fulfill_time)

    def test_stop_market_order(self):
        listeners = AsyncListeners()

        with IQFeedLevel1Listener(listeners=listeners) as level_1:
            order_request_events = SyncListeners()

            me = MockExchange(listeners=listeners,
                              order_requests_event_stream=order_request_events,
                              tick_event_stream=level_1.tick_event_filter(),
                              order_processor=StaticSlippageLoss(0.1),
                              commission_loss=PerShareCommissionLoss(0.1))

            fulfilled_orders = me.fulfilled_orders_stream()

            e1 = threading.Event()
            fulfilled_orders += lambda x: e1.set() if x.symbol == 'GOOG' else None

            e2 = threading.Event()
            fulfilled_orders += lambda x: e2.set() if x.symbol == 'AAPL' else None

            e3 = threading.Event()
            fulfilled_orders += lambda x: e3.set() if x.symbol == 'IBM' else None

            level_1.watch('GOOG')
            level_1.watch('AAPL')
            level_1.watch('IBM')

            o1 = StopMarketOrder(Type.BUY, 'GOOG', 1, 99999)
            order_request_events(o1)

            o2 = StopMarketOrder(Type.BUY, 'AAPL', 3, 99999)
            order_request_events(o2)

            o3 = StopMarketOrder(Type.SELL, 'IBM', 1, 0)
            order_request_events(o3)

            e1.wait()
            e2.wait()
            e3.wait()

        self.assertEqual(o1.obtained_quantity, 1)
        self.assertGreater(o1.cost, 0)
        self.assertIsNotNone(o1.fulfill_time)

        self.assertEqual(o2.obtained_quantity, 3)
        self.assertGreater(o2.cost, 0)
        self.assertIsNotNone(o2.fulfill_time)

        self.assertEqual(o3.obtained_quantity, 1)
        self.assertGreater(o3.cost, 0)
        self.assertIsNotNone(o3.fulfill_time)

    def test_stop_limit_order(self):
        listeners = AsyncListeners()

        with IQFeedLevel1Listener(listeners=listeners) as level_1:
            order_request_events = SyncListeners()

            me = MockExchange(listeners=listeners,
                              order_requests_event_stream=order_request_events,
                              tick_event_stream=level_1.tick_event_filter(),
                              order_processor=StaticSlippageLoss(0.1),
                              commission_loss=PerShareCommissionLoss(0.1))

            fulfilled_orders = me.fulfilled_orders_stream()

            e1 = threading.Event()
            fulfilled_orders += lambda x: e1.set() if x.symbol == 'GOOG' else None

            e2 = threading.Event()
            fulfilled_orders += lambda x: e2.set() if x.symbol == 'AAPL' else None

            e3 = threading.Event()
            fulfilled_orders += lambda x: e3.set() if x.symbol == 'IBM' else None

            level_1.watch('GOOG')
            level_1.watch('AAPL')
            level_1.watch('IBM')

            o1 = StopLimitOrder(Type.BUY, 'GOOG', 1, 99999, 1)
            order_request_events(o1)

            o2 = StopLimitOrder(Type.BUY, 'AAPL', 3, 99999, 1)
            order_request_events(o2)

            o3 = StopLimitOrder(Type.SELL, 'IBM', 1, 1, 99999)
            order_request_events(o3)

            e1.wait()
            e2.wait()
            e3.wait()

        self.assertEqual(o1.obtained_quantity, 1)
        self.assertGreater(o1.cost, 0)
        self.assertIsNotNone(o1.fulfill_time)

        self.assertEqual(o2.obtained_quantity, 3)
        self.assertGreater(o2.cost, 0)
        self.assertIsNotNone(o2.fulfill_time)

        self.assertEqual(o3.obtained_quantity, 1)
        self.assertGreater(o3.cost, 0)
        self.assertIsNotNone(o3.fulfill_time)


if __name__ == '__main__':
    unittest.main()
