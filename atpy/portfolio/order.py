import datetime
import uuid
from abc import ABCMeta
from enum import Enum

import pytz


class Type(Enum):
    BUY = 1
    SELL = 2


class BaseOrder(object, metaclass=ABCMeta):
    def __init__(self, order_type: Type, symbol: str, quantity: int, uid=None):
        self.uid = uid if uid is not None else uuid.uuid4()
        self.order_type = order_type
        self.symbol = symbol
        self.quantity = quantity

        self.__obtained_positions = list()
        self.request_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        self.__fulfill_time = None
        self.__cached_cost = None

    @property
    def fulfill_time(self):
        return self.__fulfill_time

    @fulfill_time.setter
    def fulfill_time(self, fulfill_time):
        if self.obtained_quantity != self.quantity:
            raise Exception("Order is not fulfilled. Obtained " + str(self.obtained_quantity) + " of " + str(self.quantity))

        self.__fulfill_time = fulfill_time

    @property
    def obtained_quantity(self):
        return sum([op[0] for op in self.__obtained_positions])

    def add_position(self, quantity, price):
        if self.obtained_quantity >= self.quantity:
            raise Exception("Order already fulfilled")

        self.__obtained_positions.append((quantity if self.quantity - self.obtained_quantity >= quantity else self.quantity - self.obtained_quantity, price))

        if self.obtained_quantity >= self.quantity:
            self.__fulfill_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

        self.__cached_cost = None

        return True

    @property
    def cost(self):
        if self.__cached_cost is None:
            self.__cached_cost = sum([p[0] * p[1] for p in self.__obtained_positions])

        return self.__cached_cost

    @property
    def last_cost_per_share(self):
        return self.__obtained_positions[-1][1]


class MarketOrder(BaseOrder):
    pass


class LimitOrder(BaseOrder):
    def __init__(self, order_type: Type, symbol: str, quantity: int, price: float, uid=None):
        super().__init__(order_type, symbol, quantity, uid=uid)
        self.price = price

    def add_position(self, quantity, price):
        if (self.order_type == Type.BUY and self.price < price) or (self.order_type == Type.SELL and self.price > price):
            return False

        return super().add_position(quantity, price)


class StopMarketOrder(BaseOrder):
    def __init__(self, order_type: Type, symbol: str, quantity: int, price: float, uid=None):
        super().__init__(order_type, symbol, quantity, uid=uid)

        self.price = price
        self._is_market = False

    def add_position(self, quantity, price):
        if (self.order_type == Type.BUY and self.price >= price) or (self.order_type == Type.SELL and self.price <= price):
            self._is_market = True

        return super().add_position(quantity, price) if self._is_market else False


class StopLimitOrder(BaseOrder):
    def __init__(self, order_type: Type, symbol: str, quantity: int, stop_price: float, limit_price: float, uid=None):
        super().__init__(order_type, symbol, quantity, uid=uid)

        self.stop_price = stop_price
        self.limit_price = limit_price
        self._is_limit = False

    def add_position(self, quantity, price):
        if (self.order_type == Type.BUY and self.stop_price >= price) or (self.order_type == Type.SELL and self.stop_price <= price):
            self._is_limit = True

        if self._is_limit and (self.order_type == Type.BUY and self.limit_price < price) or (self.order_type == Type.SELL and self.limit_price > price):
            return super().add_position(quantity, price)

        return False
