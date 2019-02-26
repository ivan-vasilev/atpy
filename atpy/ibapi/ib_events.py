import threading

import pandas as pd
from ibapi.client import EClient
from ibapi.common import OrderId
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.wrapper import EWrapper

import atpy.portfolio.order as orders
from atpy.portfolio.order import *


class DefaultWrapper(EWrapper):
    def __init__(self, listeners):
        EWrapper.__init__(self)

        self.listeners = listeners
        self.next_valid_order_id = -1
        self._pending_orders = dict()
        self._has_valid_id = threading.Event()
        self._lock = threading.RLock()
        self._positions = None

    def nextValidId(self, orderId: int):
        self.next_valid_order_id = orderId
        self._has_valid_id.set()

    def orderStatus(self, orderId: OrderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):

        if status == 'Filled':
            if orderId in self._pending_orders:
                order = self._pending_orders[orderId]
                del self._pending_orders[orderId]
                order.uid = orderId

                self.listeners({'type': 'order_fulfilled', 'data': order})
            else:
                self.listeners({'type': 'order_fulfilled', 'data': orderId})
        elif status in ('Inactive', 'ApiCanceled', 'Cancelled') and orderId in self._pending_orders:
            del self._pending_orders[orderId]

    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        """This event returns real-time positions for all accounts in
        response to the reqPositions() method."""

        with self._lock:
            if self._positions is None:
                self._positions = {k: list() for k in list(contract.__dict__.keys()) + ['position', 'avgCost']}

            for k, v in {**contract.__dict__, **{'position': position, 'avgCost': avgCost}}.items():
                self._positions[k].append(v)

    def positionEnd(self):
        """This is called once all position data for a given request are
        received and functions as an end marker for the position() data. """

        with self._lock:
            data = None if self._positions is None else pd.DataFrame.from_dict(self._positions)
            self._positions = None

        if data is not None:
            self.listeners({'type': 'ibapi_positions', 'data': data})

    def error(self, reqId: int, errorCode: int, errorString: str):
        super().error(reqId=reqId, errorCode=errorCode, errorString=errorString)
        self.listeners({'type': 'ibapi_error', 'data': {'reqId': reqId, 'errorCode': errorCode, 'errorString': errorString}})


class DefaultClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


class IBEvents(DefaultWrapper, DefaultClient):
    def __init__(self, listeners, ipaddress, portid, clientid):
        DefaultWrapper.__init__(self, listeners)
        DefaultClient.__init__(self, wrapper=self)

        self.listeners = listeners
        self.listeners += self.on_event

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

    def on_event(self, event):
        if event['type'] == 'order_request':
            self.process_order_request(event['data'])
        elif event['type'] == 'positions_request':
            self.reqPositions()

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

    def reqPositions(self):
        with self.lock:
            if self._positions is None:
                super().reqPositions()
