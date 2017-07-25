import threading
import unittest

import pyevents.events as events
from atpy.portfolio.order import *
from atpy.ibapi.ib_events import IBEvents


class TestIBApi(unittest.TestCase):
    """
    Test IB API Orders
    """

    def setUp(self):
        events.reset()

    def test_market_order(self):
        events.use_global_event_bus()

        e_cancel = threading.Event()

        class CustomIBEvents(IBEvents):
            def cancel_all_orders(self):
                self.reqOpenOrders()

            def openOrder(self, orderId, contract, order, orderState):
                super().openOrder(orderId, contract, order, orderState)
                if orderState.status == 'PreSubmitted':
                    self.cancelOrder(orderId)

            def openOrderEnd(self):
                super().openOrderEnd()
                e_cancel.set()

        ibe = CustomIBEvents("127.0.0.1", 4002, 0)

        with ibe:
            e1 = threading.Event()
            events.listener(lambda x: e1.set() if isinstance(x['data'], BaseOrder) and x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None)
            ibe.on_event({'type': 'order_request', 'data': MarketOrder(Type.BUY, 'GOOG', 1)})

            e2 = threading.Event()
            events.listener(lambda x: e2.set() if isinstance(x['data'], BaseOrder) and x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None)
            ibe.on_event({'type': 'order_request', 'data': MarketOrder(Type.BUY, 'AAPL', 1)})

            e1.wait()
            e2.wait()

            ibe.cancel_all_orders()

            e_cancel.wait()
