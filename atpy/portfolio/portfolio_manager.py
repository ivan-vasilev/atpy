from atpy.portfolio.order import *
from pyevents.events import *


class PortfolioManager(object):
    """Orders portfolio manager"""

    def __init__(self, initial_capital: float, uid=None, orders=None, default_listeners=None):
        self.initial_capital = initial_capital
        self._id = uid if uid is not None else uuid.uuid4()
        self.orders = orders if orders is not None else list()
        self._lock = threading.RLock()
        self._values = dict()
        self.default_listeners = default_listeners

        if default_listeners is not None:
            default_listeners += self.on_event
            self.portfolio_updated += default_listeners

    def add_order(self, order):
        with self._lock:
            if len([o for o in self.orders if o.uid == order.uid]) > 0:
                raise Exception("Attempt to fulfill existing order")

            if order.order_type == Type.SELL and self._quantity(order.symbol) < order.quantity:
                raise Exception("Attempt to sell more shares than available")

            if order.order_type == Type.BUY and self._capital < sum([p[0] * p[1] for p in order.obtained_positions]):
                raise Exception("Not enough capital to fulfill order")

            self.orders.append(order)

        self.portfolio_updated()

    @after
    def portfolio_updated(self):
        return {'type': 'portfolio_update', 'data': self}

    @property
    def capital(self):
        with self._lock:
            return self._capital

    @property
    def _capital(self):
        turnover = 0
        for o in self.orders:
            order_sum = sum([p[0] * p[1] for p in o.obtained_positions])
            if o.order_type == Type.SELL:
                turnover += order_sum
            elif o.order_type == Type.BUY:
                turnover -= order_sum

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

    def value(self, symbol=None):
        with self._lock:
            return self._value(symbol=symbol)

    def _value(self, symbol=None):
        if symbol is not None:
            if symbol not in self._values:
                logging.getLogger(__name__).debug("No current information available for " + symbol + ". Falling back to last traded price")
                symbol_orders = [o for o in self.orders if o.symbol == symbol]
                order = sorted(symbol_orders, key=lambda o: o.fulfill_time, reverse=True)[0]
                return order.obtained_positions[-1][1] * self._quantity(symbol=symbol)
            else:
                return self._values[symbol]
        else:
            result = dict()
            for s in set([o.symbol for o in self.orders]):
                result[s] = self._value(symbol=s)

            return result

    def on_event(self, event):
        if event['type'] == 'order_fulfilled':
            self.add_order(event['order'])
        elif event['type'] == 'level_1_tick':
            with self._lock:
                if event['data'].symbol in [o.symbol for o in self.orders]:
                    self._values[event['data'].symbol] = (event['data']['Ask'] + event['data']['Bid']) / 2.0

    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state['_lock']
        del state['default_listeners']

        return state

    def __setstate__(self, state):
        # Restore instance attributes (i.e., _lock).
        self.__dict__.update(state)
        self._lock = threading.RLock()
