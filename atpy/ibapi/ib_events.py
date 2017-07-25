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
        self._pending_orders = dict()
        self._has_valid_id = threading.Event()

    def nextValidId(self, orderId: int):
        self.next_valid_order_id = orderId
        self._has_valid_id.set()

    def orderStatus(self, orderId: int, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str):

        if status == 'Filled':
            if orderId in self._pending_orders:
                order = self._pending_orders[orderId]
                del self._pending_orders[orderId]
                order.uid = orderId

                self.after_event({'type': 'order_fulfilled', 'data': order})
            else:
                self.after_event({'type': 'order_fulfilled', 'data': orderId})
        elif status in ('Inactive', 'ApiCanceled', 'Cancelled') and orderId in self._pending_orders:
            del self._pending_orders[orderId]

    def error(self, reqId:int, errorCode:int, errorString:str):
        super().error(reqId=reqId, errorCode=errorCode, errorString=errorString)
        self.after_event({'type': 'ibapi_error', 'data': {'reqId': reqId, 'errorCode': errorCode, 'errorString': errorString}})

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

        thread = threading.Thread(target=self.run)
        thread.start()

        setattr(self, "_thread", thread)

        self.reqIds(-1)

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.done = True

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

            self._has_valid_id.wait()

            self._pending_orders[self.next_valid_order_id] = order

            self.placeOrder(self.next_valid_order_id, ibcontract, iborder)

            self.next_valid_order_id += 1
