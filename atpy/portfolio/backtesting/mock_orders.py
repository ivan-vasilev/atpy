import threading

import pyevents.events as events
import atpy.portfolio.order as orders


class MockOrders(object, metaclass=events.GlobalRegister):

    def __init__(self, watch_event='watch_ticks'):
        self._pending_orders = list()
        self._lock = threading.RLock()
        self._watch_event = watch_event

    @events.after
    def process_order_request(self, order):
        with self._lock:
            self._pending_orders.append(order)
            return {'type': self._watch_event, 'data': order.symbol}

    @events.after
    def order_fulfilled(self, order):
        return {'type': 'order_fulfilled', 'data': order}

    @events.listener
    def on_event(self, event):
        if event['type'] == 'order_request':
            self.process_order_request(event['data'])
        elif event['type'] == 'level_1_tick':
            self.process_tick_data(event['data'])
        elif event['type'] == 'bar':
            self.process_bar_data(event['data'])

    def process_tick_data(self, data):
        with self._lock:
            matching_orders = [o for o in self._pending_orders if o.symbol == data['symbol']]
            for order in matching_orders:
                if order.order_type == orders.Type.BUY:
                    if 'tick_id' in data:
                        order.add_position(data['last_size'], data['ask'])
                    else:
                        order.add_position(data['ask_size'] if data['ask_size'] > 0 else data['most_recent_trade_size'], data['ask'] if data['ask_size'] > 0 else data['most_recent_trade'])
                elif order.order_type == orders.Type.SELL:
                    if 'tick_id' in data:
                        order.add_position(data['last_size'], data['bid'])
                    else:
                        order.add_position(data['bid_size'] if data['bid_size'] > 0 else data['most_recent_trade_size'], data['bid'] if data['bid_size'] > 0 else data['most_recent_trade'])

                if order.fulfill_time is not None:
                    self._pending_orders.remove(order)
                    self.order_fulfilled(order)

    def process_bar_data(self, data):
        with self._lock:
            matching_orders = [o for o in self._pending_orders if o.symbol == data['symbol']]

            for order in matching_orders:
                order.add_position(data['period_volume'], data['close'])

                if order.fulfill_time is not None:
                    self._pending_orders.remove(order)
                    self.order_fulfilled(order)

