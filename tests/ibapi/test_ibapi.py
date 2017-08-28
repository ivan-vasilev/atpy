import threading
import unittest

import pyevents.events as events
from atpy.portfolio.order import *
from atpy.ibapi.ib_events import IBEvents
import pandas as pd


class TestIBApi(unittest.TestCase):
    """
    Test IB API Orders
    """

    def setUp(self):
        events.reset()

    def test_1(self):
        events.use_global_event_bus()

        e_orders = {'GOOG': threading.Event(), 'AAPL': threading.Event()}
        e_cancel = threading.Event()
        e_positions = threading.Event()

        class CustomIBEvents(IBEvents):
            def cancel_all_orders(self):
                self.reqOpenOrders()

            def openOrder(self, orderId, contract, order, orderState):
                super().openOrder(orderId, contract, order, orderState)
                if orderState.status == 'PreSubmitted':
                    self.cancelOrder(orderId)
                    e_orders[contract.symbol].set()

            def openOrderEnd(self):
                super().openOrderEnd()
                e_cancel.set()

        ibe = CustomIBEvents("127.0.0.1", 4002, 0)

        with ibe:
            events.listener(lambda x: e_orders['GOOG'].set() if isinstance(x['data'], BaseOrder) and x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None)
            ibe.on_event({'type': 'order_request', 'data': MarketOrder(Type.BUY, 'GOOG', 1)})

            events.listener(lambda x: e_orders['AAPL'].set() if isinstance(x['data'], BaseOrder) and x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None)
            ibe.on_event({'type': 'order_request', 'data': MarketOrder(Type.BUY, 'AAPL', 1)})

            events.listener(lambda x: e_positions.set() if isinstance(x['data'], pd.DataFrame) and x['type'] == 'ibapi_positions' else None)
            ibe.on_event({'type': 'positions_request'})

            for e in e_orders.values():
                e.wait()

            e_positions.wait()

            ibe.cancel_all_orders()

            e_cancel.wait()
