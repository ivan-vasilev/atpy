from pyevents.events import *


class MockOrders(object):
    def __init__(self, default_listeners=None):

        self._pending_orders = list()
        self._lock = threading.RLock()

        if default_listeners is not None:
            self.process_order += default_listeners
            default_listeners += self.on_event

    @after
    def process_order(self, order):
        with self._lock:
            self._pending_orders.append(order)
            return {'type': 'watch_symbol', 'data': order.symbol}

    def on_event(self, event):
        if event['type'] == 'order_request':
            self.process_order(event['order'])
        elif event['type'] == 'level_1_tick':
            symbol = event['data']['Symbol'].decode('ascii')
            matching_orders = [o for o in self._pending_orders if o.symbol == symbol]
            for o in matching_orders:
                self._values[event['data']['Symbol'].decode('ascii')] = event['data']['Most Recent Trade']
                self.portfolio_value_update()

    "Most Recent Trade Size"

    @after
    def order_fulfilled(self, order):
        return {'type': 'order_fulfilled', 'data': order}