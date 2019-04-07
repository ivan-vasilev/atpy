import logging
import threading
from collections import Collection

import pandas as pd

from atpy.portfolio.order import *
from pyevents.events import EventFilter


class PortfolioManager(object):
    """Orders portfolio manager"""

    def __init__(self, listeners, initial_capital: float, fulfilled_orders_event_stream, bar_event_stream=None, tick_event_stream=None, uid=None, orders=None):
        """
        :param fulfilled_orders_event_stream: event stream for fulfilled order events
        :param bar_event_stream: event stream for bar data events
        :param tick_event_stream: event stream for tick data events
        :param uid: unique id for this portfolio manager
        :param orders: a list of pre-existing orders
        """

        self.listeners = listeners

        fulfilled_orders_event_stream += self.add_order

        if bar_event_stream is not None:
            bar_event_stream += self.process_bar_data

        if tick_event_stream is not None:
            tick_event_stream += self.process_tick_data

        self.initial_capital = initial_capital
        self._id = uid if uid is not None else uuid.uuid4()
        self.orders = orders if orders is not None else list()
        self._lock = threading.RLock()
        self._values = dict()

    def add_order(self, order: BaseOrder):
        with self._lock:
            if order.fulfill_time is None:
                raise Exception("Order has no fulfill_time set")

            if len([o for o in self.orders if o.uid == order.uid]) > 0:
                raise Exception("Attempt to fulfill existing order")

            if order.order_type == Type.SELL and self._quantity(order.symbol) < order.quantity:
                raise Exception("Attempt to sell more shares than available")

            if order.order_type == Type.BUY and self._capital < order.cost:
                raise Exception("Not enough capital to fulfill order")

            self.orders.append(order)

            self.listeners({'type': 'watch_ticks', 'data': order.symbol})
            self.listeners({'type': 'portfolio_update', 'data': self})

    def portfolio_updates_stream(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if ('type' in e and e['type'] == 'portfolio_update') else False,
                           event_transformer=lambda e: (e['data'],))

    @property
    def symbols(self):
        """Get list of all orders/symbols"""

        return set([o.symbol for o in self.orders])

    @property
    def capital(self):
        """Get available capital (including orders)"""

        with self._lock:
            return self._capital

    @property
    def _capital(self):
        turnover = 0
        commissions = 0
        for o in self.orders:
            cost = o.cost
            if o.order_type == Type.SELL:
                turnover += cost
            elif o.order_type == Type.BUY:
                turnover -= cost

            commissions += o.commission

        return self.initial_capital + turnover - commissions

    def quantity(self, symbol=None):
        with self._lock:
            return self._quantity(symbol=symbol)

    def _quantity(self, symbol=None):
        if symbol is not None:
            quantity = 0

            for o in [o for o in self.orders if o.symbol == symbol]:
                if o.order_type == Type.BUY:
                    quantity += o.quantity
                elif o.order_type == Type.SELL:
                    quantity -= o.quantity

            return quantity
        else:
            result = dict()
            for s in set([o.symbol for o in self.orders]):
                qty = self._quantity(s)
                if qty > 0:
                    result[s] = qty

            return result

    def value(self, symbol=None, multiply_by_quantity=False):
        with self._lock:
            return self._value(symbol=symbol, multiply_by_quantity=multiply_by_quantity)

    def _value(self, symbol=None, multiply_by_quantity=False):
        if symbol is not None:
            if symbol not in self._values:
                logging.getLogger(__name__).debug("No current information available for %s. Falling back to last traded price" % symbol)
                symbol_orders = [o for o in self.orders if o.symbol == symbol]
                order = sorted(symbol_orders, key=lambda o: o.fulfill_time, reverse=True)[0]
                return order.last_cost_per_share * (self._quantity(symbol=symbol) if multiply_by_quantity else 1)
            else:
                return self._values[symbol] * (self._quantity(symbol=symbol) if multiply_by_quantity else 1)
        else:
            result = dict()
            for s in set([o.symbol for o in self.orders]):
                result[s] = self._value(symbol=s, multiply_by_quantity=multiply_by_quantity)

            return result

    def process_tick_data(self, data):
        with self._lock:
            symbol = data['symbol']
            if symbol in [o.symbol for o in self.orders]:
                self._values[symbol] = data['bid'][-1] if isinstance(data['bid'], Collection) else data['bid']
                self.listeners({'type': 'portfolio_value_update', 'data': self})

    def process_bar_data(self, data):
        with self._lock:
            symbols = data.index.get_level_values(level='symbol')

            for o in [o for o in self.orders if o.symbol in symbols]:
                slc = data.loc[pd.IndexSlice[:, o.symbol], 'close']
                if not slc.empty:
                    self._values[o.symbol] = slc[-1]
                    self.listeners({'type': 'portfolio_value_update', 'data': self})

    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state['_lock']
        del state['listeners']

        return state

    def __setstate__(self, state):
        # Restore instance attributes (i.e., _lock).
        self.__dict__.update(state)
        self._lock = threading.RLock()
