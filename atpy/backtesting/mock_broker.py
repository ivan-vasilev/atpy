import threading

import pandas as pd

import atpy.portfolio.order as orders


class MockOrders(object):

    def __init__(self, listeners, watch_event='watch_ticks'):
        self.listeners = listeners
        self.listeners += self.on_event

        self._pending_orders = list()
        self._lock = threading.RLock()
        self._watch_event = watch_event

    def process_order_request(self, order):
        with self._lock:
            self._pending_orders.append(order)
            self.listeners({'type': self._watch_event, 'data': order.symbol})

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
                    self.listeners({'type': 'order_fulfilled', 'data': order})

    def process_bar_data(self, data):
        with self._lock:
            symbols = [o.symbol for o in self._pending_orders]
            for o in [o for o, isin in zip(self._pending_orders, data.index.isin(symbols, level=1)) if isin]:
                datum = data.loc[pd.IndexSlice[:, o.symbol], :].iloc[-1]
                o.add_position(datum['period_volume'], datum['close'])

                if o.fulfill_time is not None:
                    self._pending_orders.remove(o)
                    self.listeners({'type': 'order_fulfilled', 'data': o})
