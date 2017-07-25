from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.order import Order
from ibapi.contract import Contract

import pyevents.events as events
import atpy.portfolio.order as orders
import threading
from atpy.portfolio.order import *


class DefaultWrapper(EWrapper, metaclass=events.GlobalRegister):
    def __init__(self):
        EWrapper.__init__(self)
        self.next_valid_order_id = -1
        self.pending_orders = dict()

    def nextValidId(self, orderId: int):
        self.next_valid_order_id = orderId

    def orderStatus(self, orderId: int, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str):

        if status == 'Filled':
            order = self.pending_orders[orderId]
            del self.pending_orders[orderId]
            order.uid = orderId

            self.after_event({'type': 'order_fulfilled', 'data': order})

    @events.after
    def after_event(self, data):
        return data


class DefaultClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


class IBEvents(DefaultWrapper, DefaultClient, metaclass=events.GlobalRegister):
    def __init__(self, ipaddress, portid, clientid):
        DefaultWrapper.__init__(self)
        DefaultClient.__init__(self, wrapper=self)

        self.ipaddress = ipaddress
        self.portid = portid
        self.clientid = clientid
        self.lock = threading.RLock()

    def __enter__(self):
        self.connect(self.ipaddress, self.portid, self.clientid)

        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()

        setattr(self, "_thread", thread)

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.disconnect()

    @events.listener
    def on_event(self, event):
        if event['type'] == 'order_request':
            self.process_order_request(event['data'])

    def process_order_request(self, order):
        with self.lock:
            ibcontract = Contract()
            ibcontract.symbol = order.symbol
            ibcontract.secType = "STK"
            ibcontract.currency = "USD"
            ibcontract.exchange = "SMART"

            iborder = Order()

            iborder.action = 'BUY' if order.order_type == Type.BUY else 'SELL' if order.order_type == Type.SELL else None

            if isinstance(order, orders.MarketOrder):
                iborder.orderType = "MKT"
            elif isinstance(order, orders.LimitOrder):
                iborder.orderType = "LMT"
                order.lmtPrice = order.price
            elif isinstance(order, orders.StopMarketOrder):
                iborder.orderType = "STP"
                order.auxPrice = order.price
            elif isinstance(order, orders.StopLimitOrder):
                iborder.orderType = "STP LMT"
                order.lmtPrice = order.limit_price
                order.auxPrice = order.stop_price

            iborder.totalQuantity = order.quantity

            self.pending_orders[self.next_valid_order_id] = order

            self.placeOrder(self.next_valid_order_id, ibcontract, iborder)
