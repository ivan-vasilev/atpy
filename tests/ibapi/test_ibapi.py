import threading
import unittest

import pandas as pd

from atpy.ibapi.ib_events import IBEvents
from atpy.portfolio.order import *
from pyevents.events import AsyncListeners


class TestIBApi(unittest.TestCase):
    """
    Test IB API Orders
    """

    def test_1(self):
        e_orders = {'GOOG': threading.Event(), 'AAPL': threading.Event()}
        e_cancel = threading.Event()
        e_positions = threading.Event()

        listeners = AsyncListeners()

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

        ibe = CustomIBEvents(listeners=listeners, ipaddress="127.0.0.1", portid=4002, clientid=0)

        with ibe:
            listeners += lambda x: e_orders['GOOG'].set() if isinstance(x['data'], BaseOrder) and x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None
            listeners({'type': 'order_request', 'data': MarketOrder(Type.BUY, 'GOOG', 1)})

            listeners += lambda x: e_orders['AAPL'].set() if isinstance(x['data'], BaseOrder) and x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None
            listeners({'type': 'order_request', 'data': MarketOrder(Type.BUY, 'AAPL', 1)})

            listeners += lambda x: e_positions.set() if isinstance(x['data'], pd.DataFrame) and x['type'] == 'ibapi_positions' else None
            listeners({'type': 'positions_request', 'data': None})

            for e in e_orders.values():
                e.wait()

            e_positions.wait()

            ibe.cancel_all_orders()

            e_cancel.wait()
