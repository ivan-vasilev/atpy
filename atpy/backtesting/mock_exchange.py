import logging
import threading
import typing

import pandas as pd

import atpy.portfolio.order as orders


class MockExchange(object):
    """
    Mock exchange for executing trades based on the current streaming prices. Works with realtime and historical data.
    """

    def __init__(self,
                 listeners,
                 accept_bars: typing.Callable = None,
                 accept_ticks: typing.Callable = None,
                 slippage_loss: typing.Callable = None,
                 commission_loss: typing.Callable = None):
        """
        Add source for data generation
        :param listeners: listeners
        :param accept_bars: treat event like bar data. Has to return the bar dataframe. If None, then the event is not accepted
        :param accept_ticks: treat event like tick data. Has to return the tick dataframe. If None, then the event is not accepted
        :param slippage_loss: apply slippage loss to the price
        :param commission_loss: apply commission loss to the price
        """

        if accept_bars is not None:
            self.accept_bars = accept_bars
        else:
            self.accept_bars = lambda e: e['data'] if e['type'] == 'bar' else None

        if accept_ticks is not None:
            self.accept_ticks = accept_ticks
        else:
            self.accept_ticks = lambda e: e['data'] if e['type'] == 'level_1_tick' else None

        self.listeners = listeners
        self.listeners += self.on_event

        self.slippage_loss = slippage_loss if slippage_loss is not None else lambda x: 0
        self.commission_loss = commission_loss if commission_loss is not None else lambda x: 0

        self._pending_orders = list()
        self._slippages = dict()
        self._lock = threading.RLock()

    def process_order_request(self, order):
        with self._lock:
            self._pending_orders.append(order)
            self._slippages[order] = list()

    def on_event(self, event):
        if event['type'] == 'order_request':
            self.process_order_request(event['data'])
        elif self.accept_ticks(event) is not None:
            self.process_tick_data(self.accept_ticks(event))
        elif self.accept_bars(event) is not None:
            self.process_bar_data(self.accept_bars(event))

    def process_tick_data(self, data):
        with self._lock:
            matching_orders = [o for o in self._pending_orders if o.symbol == data['symbol']]
            for o in matching_orders:
                if o.order_type == orders.Type.BUY:
                    if 'tick_id' in data:
                        price = data['ask']
                        slippage = self.slippage_loss(o, price)

                        o.add_position(data['last_size'], price + slippage)
                    else:
                        price = data['ask'] if data['ask_size'] > 0 else data['most_recent_trade']
                        slippage = self.slippage_loss(o, price)

                        o.add_position(data['ask_size'] if data['ask_size'] > 0 else data['most_recent_trade_size'], price + slippage)

                    self._slippages[o].append(slippage)

                elif o.order_type == orders.Type.SELL:
                    if 'tick_id' in data:
                        price = data['bid']
                        slippage = self.slippage_loss(o, price)

                        o.add_position(data['last_size'], price + slippage)
                    else:
                        price = data['bid'] if data['bid_size'] > 0 else data['most_recent_trade']
                        slippage = self.slippage_loss(o, price)
                        o.add_position(data['bid_size'] if data['bid_size'] > 0 else data['most_recent_trade_size'], price + slippage)

                    self._slippages[o].append(slippage)

                if o.fulfill_time is not None:
                    self._pending_orders.remove(o)
                    slippages = ', '.join(["%.2f" % s for s in self._slippages.pop(o)])

                    logging.getLogger(__name__).info("Order fulfilled: " + str(o) + " with slippage: " + slippages)

                    self.listeners({'type': 'order_fulfilled', 'data': o})

    def process_bar_data(self, data):
        with self._lock:
            symbol_ind = data.index.names.index('symbol')

            for o in [o for o in self._pending_orders if o.symbol in data.index.levels[symbol_ind]]:
                slc = data.loc[pd.IndexSlice[:, o.symbol], :]
                if not slc.empty:
                    price = slc.iloc[-1]['close']
                    slippage = self.slippage_loss(o, price)

                    o.add_position(slc.iloc[-1]['period_volume'], price + slippage)

                    self._slippages[o].append(slippage)

                    if o.fulfill_time is not None:
                        self._pending_orders.remove(o)
                        slippages = ', '.join(["%.2f" % s for s in self._slippages.pop(o)])
                        logging.getLogger(__name__).info("Order fulfilled: " + str(o) + " with slippage: " + slippages)

                        self.listeners({'type': 'order_fulfilled', 'data': o})


class StaticSlippageLoss:
    """Apply static loss value to account for slippage per each order"""

    def __init__(self, loss_rate):
        self.loss_rate = loss_rate

    def __call__(self, o: orders.BaseOrder, price: float):
        if o.order_type == orders.Type.BUY:
            return self.loss_rate * price
        elif o.order_type == orders.Type.SELL:
            return -self.loss_rate * price
