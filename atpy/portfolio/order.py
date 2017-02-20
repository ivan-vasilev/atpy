from enum import Enum
import uuid
import datetime
from abc import ABCMeta


class Type(Enum):
    BUY = 1
    SELL = 2


class BaseOrder(object, metaclass=ABCMeta):
    def __init__(self, order_type: Type, symbol: str, quantity: int, uid=None):
        self.uid = uid if uid is not None else uuid.uuid4()
        self.order_type = order_type
        self.symbol = symbol
        self.quantity = quantity

        self.obtained_positions = list()
        self.request_time = datetime.datetime.now()
        self.fulfill_time = None


class MarketOrder(BaseOrder):
    pass


class LimitOrder(BaseOrder):
    def __init__(self, order_type: Type, symbol: str, quantity: int, price: float):
        super().__init__(order_type, symbol, quantity)

        self.price = price


class StopMarketOrder(BaseOrder):
    def __init__(self, order_type: Type, symbol: str, quantity: int, stop_price: float):
        super().__init__(order_type, symbol, quantity)

        self.stop_price = stop_price


class StopLimitOrder(BaseOrder):
    def __init__(self, order_type: Type, symbol: str, quantity: int, limit_price: float, stop_price: float):
        super().__init__(order_type, symbol, quantity)

        self.limit_price = limit_price
        self.stop_price = stop_price
