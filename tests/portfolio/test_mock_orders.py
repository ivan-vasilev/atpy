import unittest

from atpy.data.iqfeed.iqfeed_level_1_provider import *
from atpy.portfolio.backtesting.mock_orders import *
from atpy.portfolio.order import *


class TestMockOrders(unittest.TestCase):
    """
    Test Mock Orders
    """

    def setUp(self):
        events.reset()

    def test_market_order(self):
        events.use_global_event_bus()

        with IQFeedLevel1Listener(column_mode=True):
            mock_orders = MockOrders()

            e1 = threading.Event()

            def callback_1(x):
                print('callback_1' + str(x))
                if ['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG':
                    print('E1')
                    e1.set()

            events.listener(callback_1)

            e2 = threading.Event()

            def callback_2(x):
                if ['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL':
                    print('E2')
                    e2.set()

            events.listener(callback_2)

            o1 = MarketOrder(Type.BUY, 'GOOG', 1)
            mock_orders.on_event({'type': 'order_request', 'data': o1})

            o2 = MarketOrder(Type.BUY, 'AAPL', 3)
            mock_orders.on_event({'type': 'order_request', 'data': o2})

            e3 = threading.Event()

            def callback_3(x):
                if ['type'] == 'order_fulfilled' and x['data'].symbol == 'MSFT':
                    print('E3')
                    e3.set()

            events.listener(callback_3)
            o3 = MarketOrder(Type.SELL, 'MSFT', 1)
            mock_orders.on_event({'type': 'order_request', 'data': o3})
            e3.wait()

            e1.wait()
            e2.wait()

        self.assertEqual(o1.obtained_quantity, 1)
        self.assertNotEqual(o1.cost, 0)
        self.assertNotEqual(o1.fulfill_time, 0)

        self.assertEqual(o2.obtained_quantity, 3)
        self.assertNotEqual(o2.cost, 0)
        self.assertNotEqual(o2.fulfill_time, 0)

        self.assertEqual(o3.obtained_quantity, 1)
        self.assertNotEqual(o3.cost, 0)
        self.assertNotEqual(o3.fulfill_time, 0)

    def test_limit_order(self):
        events.use_global_event_bus()

        with IQFeedLevel1Listener(column_mode=True):
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
        self.assertNotEqual(o1.cost, 0)
        self.assertNotEqual(o1.fulfill_time, 0)

        self.assertEqual(o2.obtained_quantity, 3)
        self.assertNotEqual(o2.cost, 0)
        self.assertNotEqual(o2.fulfill_time, 0)

        self.assertEqual(o3.obtained_quantity, 1)
        self.assertNotEqual(o3.cost, 0)
        self.assertNotEqual(o3.fulfill_time, 0)

if __name__ == '__main__':
    unittest.main()
