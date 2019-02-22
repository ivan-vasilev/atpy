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
                 order_processor: typing.Callable = None,
                 commission_loss: typing.Callable = None):
        """
        Add source for data generation
        :param listeners: listeners
        :param accept_bars: treat event like bar data. Has to return the bar dataframe. If None, then the event is not accepted
        :param accept_ticks: treat event like tick data. Has to return the tick dataframe. If None, then the event is not accepted
        :param order_processor: a function which takes the current bar/tick volume and price and the current order.
                                It applies some logic to return allowed volume and price for the order, given the current conditions.
                                This function might apply slippage and so on
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

        self.order_processor = order_processor if order_processor is not None else lambda order, price, volume: (price, volume)
        self.commission_loss = commission_loss if commission_loss is not None else lambda o: 0

        self._pending_orders = list()
        self._lock = threading.RLock()

    def process_order_request(self, order):
        with self._lock:
            self._pending_orders.append(order)

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
                        price, volume = self.order_processor(o, data['ask'], data['last_size'])

                        o.add_position(volume, price)
                    else:
                        price, volume = self.order_processor(order=o,
                                                             price=data['ask'] if data['ask_size'] > 0 else data['most_recent_trade'],
                                                             volume=data['ask_size'] if data['ask_size'] > 0 else data['most_recent_trade_size'])

                        o.add_position(volume, price)

                    o.commission = self.commission_loss(o)

                elif o.order_type == orders.Type.SELL:
                    if 'tick_id' in data:
                        price, volume = self.order_processor(o, data['bid'], data['last_size'])
                        o.add_position(price, volume)
                    else:
                        price, volume = self.order_processor(order=o,
                                                             price=data['bid'] if data['bid_size'] > 0 else data['most_recent_trade'],
                                                             volume=data['bid_size'] if data['bid_size'] > 0 else data['most_recent_trade_size'])

                        o.add_position(price, volume)

                    o.commission = self.commission_loss(o)

                if o.fulfill_time is not None:
                    self._pending_orders.remove(o)

                    logging.getLogger(__name__).info("Order fulfilled: " + str(o))

                    self.listeners({'type': 'order_fulfilled', 'data': o})

    def process_bar_data(self, data):
        with self._lock:
            symbol_ind = data.index.names.index('symbol')

            for o in [o for o in self._pending_orders if o.symbol in data.index.levels[symbol_ind]]:
                slc = data.loc[pd.IndexSlice[:, o.symbol], :]
                if not slc.empty:
                    price, volume = self.order_processor(o, slc.iloc[-1]['close'], slc.iloc[-1]['period_volume'])

                    o.add_position(volume, price)

                    o.commission = self.commission_loss(o)

                    if o.fulfill_time is not None:
                        self._pending_orders.remove(o)
                        logging.getLogger(__name__).info("Order fulfilled: " + str(o))

                        self.listeners({'type': 'order_fulfilled', 'data': o})


class StaticSlippageLoss:
    """Apply static loss value to account for slippage per each order"""

    def __init__(self, loss_rate: float, max_order_volume: float = 1.0):
        """
        :param loss_rate: slippage loss rate [0:1] coefficient for each order
        :param max_order_volume: [0:1] coefficient, which says how much of the available volume can be assigned to this order
        """
        self.loss_rate = loss_rate
        self.max_order_volume = max_order_volume

    def __call__(self, order: orders.BaseOrder, price: float, volume: int):
        if order.order_type == orders.Type.BUY:
            return price + self.loss_rate * price, int(volume * self.max_order_volume)
        elif order.order_type == orders.Type.SELL:
            return price - self.loss_rate * price, int(volume * self.max_order_volume)


class PerShareCommissionLoss:
    """Apply commission loss for each share"""

    def __init__(self, value):
        self.value = value

    def __call__(self, o: orders.BaseOrder):
        return o.obtained_quantity * self.value
