import logging
import random

import pandas as pd

from atpy.portfolio.portfolio_manager import PortfolioManager, MarketOrder, Type
from pyevents.events import EventFilter


class RandomStrategy:
    """Random buy/sell on each step"""

    def __init__(self, listeners, bar_event_stream, portfolio_manager: PortfolioManager, max_buys_per_step=1, max_sells_per_step=1):
        """
        :param listeners: listeners environment
        :param bar_event_stream: bar events
        :param portfolio_manager: Portfolio manager
        :param max_buys_per_step: maximum buy orders per time step (one bar)
        :param max_sells_per_step: maximum sell orders per time step (one bar)
        """
        self.listeners = listeners
        bar_event_stream += self.on_bar_event

        self.portfolio_manager = portfolio_manager
        self.max_buys_per_step = max_buys_per_step
        self.max_sells_per_step = max_sells_per_step

    def on_bar_event(self, data):
        buys = random.randint(0, min(len(data.index.get_level_values('symbol')), self.max_buys_per_step))

        for _ in range(buys):
            symbol = data.sample().index.get_level_values('symbol')[0]
            volume = random.randint(1, data.loc[pd.IndexSlice[:, symbol], :].iloc[-1]['volume'])

            o = MarketOrder(Type.BUY, symbol, volume)

            logging.getLogger(__name__).debug('Placing new order ' + str(o))

            self.listeners({'type': 'order_request', 'data': o})

        quantities = self.portfolio_manager.quantity()
        sells = random.randint(0, min(len(quantities), self.max_sells_per_step))

        selected_symbols = set()
        orders = list()
        for _ in range(sells):
            symbol, volume = random.choice(list(quantities.items()))
            while symbol in selected_symbols:
                symbol, volume = random.choice(list(quantities.items()))

            selected_symbols.add(symbol)
            orders.append(MarketOrder(Type.SELL, symbol, random.randint(1, min(self.portfolio_manager.quantity(symbol), volume))))

        for o in orders:
            logging.getLogger(__name__).debug('Placing new order ' + str(o))
            self.listeners({'type': 'order_request', 'data': o})

    def order_requests_stream(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if ('type' in e and e['type'] == 'order_request') else False,
                           event_transformer=lambda e: (e['data'],))
