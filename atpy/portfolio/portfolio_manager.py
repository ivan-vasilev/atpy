from atpy.portfolio.order import *
import pyevents.events as events
import threading
import logging


class PortfolioManager(object, metaclass=events.GlobalRegister):
    """Orders portfolio manager"""

    def __init__(self, initial_capital: float, uid=None, orders=None):
        self.initial_capital = initial_capital
        self._id = uid if uid is not None else uuid.uuid4()
        self.orders = orders if orders is not None else list()
        self._lock = threading.RLock()
        self._values = dict()

    @events.after
    def add_order(self, order):
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

            return events.CompositeEvent([{'type': 'watch_ticks', 'data': order.symbol}, {'type': 'portfolio_update', 'data': self}])

    @property
    def symbols(self):
        return set([o.symbol for o in self.orders])

    @property
    def capital(self):
        with self._lock:
            return self._capital

    @property
    def _capital(self):
        turnover = 0
        for o in self.orders:
            cost = o.cost
            if o.order_type == Type.SELL:
                turnover += cost
            elif o.order_type == Type.BUY:
                turnover -= cost

        return self.initial_capital + turnover

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
                result[s] = self._quantity(s)

            return result

    def value(self, symbol=None, multiply_by_quantity=False):
        with self._lock:
            return self._value(symbol=symbol, multiply_by_quantity=multiply_by_quantity)

    def _value(self, symbol=None, multiply_by_quantity=False):
        if symbol is not None:
            if symbol not in self._values:
                logging.getLogger(__name__).debug("No current information available for " + symbol + ". Falling back to last traded price")
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

    @events.listener
    def on_event(self, event):
        if event['type'] == 'order_fulfilled':
            self.add_order(event['data'])
        elif event['type'] == 'level_1_tick':
            with self._lock:
                if event['data']['symbol'] in [o.symbol for o in self.orders]:
                    self._values[event['data']['symbol']] = event['data']['Bid']
                    self.portfolio_value_update()
        elif event['type'] == 'bar':
            with self._lock:
                if event['data']['symbol'] in [o.symbol for o in self.orders]:
                    self._values[event['data']['symbol']] = event['data']['close']
                    self.portfolio_value_update()

    @events.after
    def portfolio_value_update(self):
        return {'type': 'portfolio_value_update', 'data': self}

    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state['_lock']

        return state

    def __setstate__(self, state):
        # Restore instance attributes (i.e., _lock).
        self.__dict__.update(state)
        self._lock = threading.RLock()
