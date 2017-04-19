import unittest

from atpy.data.iqfeed.iqfeed_level_1_provider import *
from atpy.data.iqfeed.iqfeed_history_provider import *
from atpy.portfolio.backtesting.mock_orders import *
from atpy.portfolio.order import *
from atpy.data.iqfeed.iqfeed_bar_data_provider import *


class TestMockOrders(unittest.TestCase):
    """
    Test Mock Orders
    """

    def setUp(self):
        events.reset()

    def test_market_order(self):
        events.use_global_event_bus()

        with IQFeedLevel1Listener():
            mock_orders = MockOrders()

            e1 = threading.Event()
            events.listener(lambda x: e1.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None)

            e2 = threading.Event()
            events.listener(lambda x: e2.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None)

            o1 = MarketOrder(Type.BUY, 'GOOG', 1)
            mock_orders.on_event({'type': 'order_request', 'data': o1})

            o2 = MarketOrder(Type.BUY, 'AAPL', 3)
            mock_orders.on_event({'type': 'order_request', 'data': o2})

            e3 = threading.Event()
            events.listener(lambda x: e3.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'IBM' else None)
            o3 = MarketOrder(Type.SELL, 'IBM', 1)
            mock_orders.on_event({'type': 'order_request', 'data': o3})
            e3.wait()

            e1.wait()
            e2.wait()

        self.assertEqual(o1.obtained_quantity, 1)
        self.assertGreater(o1.cost, 0)
        self.assertIsNotNone(o1.fulfill_time)

        self.assertEqual(o2.obtained_quantity, 3)
        self.assertGreater(o2.cost, 0)
        self.assertIsNotNone(o2.fulfill_time)

        self.assertEqual(o3.obtained_quantity, 1)
        self.assertGreater(o3.cost, 0)
        self.assertIsNotNone(o3.fulfill_time)

    def test_historical_market_order(self):
        events.use_global_event_bus()

        mock_orders = MockOrders()

        e1 = threading.Event()
        events.listener(lambda x: e1.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None)

        e2 = threading.Event()
        events.listener(lambda x: e2.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None)

        o1 = MarketOrder(Type.BUY, 'GOOG', 1)
        mock_orders.on_event({'type': 'order_request', 'data': o1})

        o2 = MarketOrder(Type.BUY, 'AAPL', 3)
        mock_orders.on_event({'type': 'order_request', 'data': o2})

        e3 = threading.Event()
        events.listener(lambda x: e3.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'IBM' else None)
        o3 = MarketOrder(Type.SELL, 'IBM', 1)
        mock_orders.on_event({'type': 'order_request', 'data': o3})

        filter_provider = DefaultFilterProvider()
        filter_provider += TicksFilter(ticker="GOOG", max_ticks=1)
        filter_provider += TicksFilter(ticker="AAPL", max_ticks=1)
        filter_provider += TicksFilter(ticker="IBM", max_ticks=1)

        with IQFeedHistoryListener(fire_ticks=True, filter_provider=filter_provider):
            e3.wait()
            e1.wait()
            e2.wait()

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
        events.use_global_event_bus()

        with IQFeedBarDataListener(minibatch=2) as listener:
            mock_orders = MockOrders(watch_event='watch_bars')

            e1 = threading.Event()
            events.listener(lambda x: e1.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None)

            e2 = threading.Event()
            events.listener(lambda x: e2.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None)

            o1 = MarketOrder(Type.BUY, 'GOOG', 1)
            mock_orders.on_event({'type': 'order_request', 'data': o1})

            o2 = MarketOrder(Type.BUY, 'AAPL', 3)
            mock_orders.on_event({'type': 'order_request', 'data': o2})

            e3 = threading.Event()
            events.listener(lambda x: e3.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'IBM' else None)
            o3 = MarketOrder(Type.SELL, 'IBM', 1)
            mock_orders.on_event({'type': 'order_request', 'data': o3})

            listener.watch(symbol='GOOG', interval_len=5, interval_type='s', update=1, lookback_bars=10)
            listener.watch(symbol='AAPL', interval_len=5, interval_type='s', update=1, lookback_bars=10)
            listener.watch(symbol='IBM', interval_len=5, interval_type='s', update=1, lookback_bars=10)

            e3.wait()
            e1.wait()
            e2.wait()

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
        events.use_global_event_bus()

        mock_orders = MockOrders()

        e1 = threading.Event()
        events.listener(lambda x: e1.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None)

        e2 = threading.Event()
        events.listener(lambda x: e2.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None)

        o1 = MarketOrder(Type.BUY, 'GOOG', 1)
        mock_orders.on_event({'type': 'order_request', 'data': o1})

        o2 = MarketOrder(Type.BUY, 'AAPL', 3)
        mock_orders.on_event({'type': 'order_request', 'data': o2})

        e3 = threading.Event()
        events.listener(lambda x: e3.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'IBM' else None)
        o3 = MarketOrder(Type.SELL, 'IBM', 1)
        mock_orders.on_event({'type': 'order_request', 'data': o3})

        filter_provider = DefaultFilterProvider()
        filter_provider += BarsFilter(ticker="GOOG", interval_len=60, interval_type='s', max_bars=20)
        filter_provider += BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=20)
        filter_provider += BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=20)

        with IQFeedHistoryListener(fire_ticks=True, filter_provider=filter_provider):
            e3.wait()
            e1.wait()
            e2.wait()

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
        events.use_global_event_bus()

        with IQFeedLevel1Listener():
            mock_orders = MockOrders()

            e1 = threading.Event()
            events.listener(lambda x: e1.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None)

            e2 = threading.Event()
            events.listener(lambda x: e2.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None)

            e3 = threading.Event()
            events.listener(lambda x: e3.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'IBM' else None)

            o1 = LimitOrder(Type.BUY, 'GOOG', 1, 99999)
            mock_orders.on_event({'type': 'order_request', 'data': o1})

            o2 = LimitOrder(Type.BUY, 'AAPL', 3, 99999)
            mock_orders.on_event({'type': 'order_request', 'data': o2})

            o3 = LimitOrder(Type.SELL, 'IBM', 1, 0)
            mock_orders.on_event({'type': 'order_request', 'data': o3})

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
        events.use_global_event_bus()

        with IQFeedLevel1Listener():
            mock_orders = MockOrders()

            e1 = threading.Event()
            events.listener(lambda x: e1.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None)

            e2 = threading.Event()
            events.listener(lambda x: e2.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None)

            e3 = threading.Event()
            events.listener(lambda x: e3.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'IBM' else None)

            o1 = StopMarketOrder(Type.BUY, 'GOOG', 1, 99999)
            mock_orders.on_event({'type': 'order_request', 'data': o1})

            o2 = StopMarketOrder(Type.BUY, 'AAPL', 3, 99999)
            mock_orders.on_event({'type': 'order_request', 'data': o2})

            o3 = StopMarketOrder(Type.SELL, 'IBM', 1, 0)
            mock_orders.on_event({'type': 'order_request', 'data': o3})

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
        events.use_global_event_bus()

        with IQFeedLevel1Listener():
            mock_orders = MockOrders()

            e1 = threading.Event()
            events.listener(lambda x: e1.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None)

            e2 = threading.Event()
            events.listener(lambda x: e2.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None)

            e3 = threading.Event()
            events.listener(lambda x: e3.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'IBM' else None)

            o1 = StopLimitOrder(Type.BUY, 'GOOG', 1, 99999, 1)
            mock_orders.on_event({'type': 'order_request', 'data': o1})

            o2 = StopLimitOrder(Type.BUY, 'AAPL', 3, 99999, 1)
            mock_orders.on_event({'type': 'order_request', 'data': o2})

            o3 = StopLimitOrder(Type.SELL, 'IBM', 1, 1, 99999)
            mock_orders.on_event({'type': 'order_request', 'data': o3})

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
