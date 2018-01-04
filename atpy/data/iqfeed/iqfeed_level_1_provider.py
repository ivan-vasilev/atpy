import queue
import threading
import typing
from typing import Sequence

import numpy as np
import pandas as pd
import logging
import pyevents.events as events
import pyiqfeed as iq
from atpy.data.iqfeed.util import launch_service, iqfeed_to_dict, create_batch, IQFeedDataProvider


class IQFeedLevel1Listener(iq.SilentQuoteListener, metaclass=events.GlobalRegister):

    def __init__(self, fire_ticks=True, minibatch=None, conn: iq.QuoteConn=None, key_suffix=''):
        super().__init__(name="Level 1 listener")

        self.fire_ticks = fire_ticks
        self.minibatch = minibatch
        self.conn = conn
        self._own_conn = conn is None
        self.key_suffix = key_suffix

        self.fundamentals = dict()

        self.current_news_mb = list()
        self.current_regional_mb = None
        self.current_update_mb = None
        self._lock = threading.RLock()

    def __enter__(self):
        if self._own_conn:
            launch_service()
            self.conn = iq.QuoteConn()
            self.conn.add_listener(self)
            self.conn.connect()
        else:
            self.conn.add_listener(self)

        self.queue = queue.Queue()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.conn.remove_listener(self)

        if self._own_conn:
            self.conn.disconnect()

        self.conn = None

    def __del__(self):
        if self.conn is not None:
            self.conn.remove_listener(self)
            self.conn.disconnect()

    def __getattr__(self, name):
        if self.conn is not None:
            return getattr(self.conn, name)
        else:
            raise AttributeError

    @events.listener
    def on_event(self, event):
        if event['type'] == 'watch_ticks':
            with self._lock:
                if event['data'] not in self.fundamentals:
                    self.conn.watch(event['data'])

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        """
        You made a subscription request with an invalid symbol
        :param bad_symbol: The bad symbol
        """

        logging.getLogger(__name__).warning("Invalid symbol request: " + str(bad_symbol))

        with self._lock:
            if bad_symbol in self.fundamentals:
                if isinstance(self.fundamentals[bad_symbol], threading.Event):
                    self.fundamentals[bad_symbol].set()

                del self.fundamentals[bad_symbol]

    def process_news(self, news_item: iq.QuoteConn.NewsMsg):
        news_item = news_item._asdict()

        self.on_news(news_item)

        if self.minibatch is not None:
            self.current_news_mb.append(news_item)
            if len(self.current_news_mb) == self.minibatch:
                processed_data = {f + self.key_suffix: list() for f in news_item.keys()}
                for ni in self.current_news_mb:
                    for k, v in ni.items():
                        processed_data[k].append(v)

                self.on_news_mb(processed_data)

                self.current_news_mb = list()

    @events.after
    def on_news(self, news_item: iq.QuoteConn.NewsMsg):
        return {'type': 'level_1_news_item', 'data': news_item}

    @events.after
    def on_news_mb(self, news_list):
        return {'type': 'level_1_news_batch', 'data': news_list}

    def news_provider(self):
        return IQFeedDataProvider(self.on_news)

    def process_watched_symbols(self, symbols: Sequence[str]) -> None:
        """List of all watched symbols when requested."""
        self.watched_symbols = symbols

    def process_regional_quote(self, quote: np.array):
        if self.fire_ticks:
            self.on_regional_quote(iqfeed_to_dict(quote, self.key_suffix))

        if self.minibatch is not None:
            if self.current_regional_mb is None:
                self.current_regional_mb = np.empty((self.minibatch,), dtype=self.conn.regional_type)
                self._current_regional_mb_index = 0

            self.current_regional_mb[self._current_regional_mb_index] = quote
            self._current_regional_mb_index += 1

            if len(self.current_regional_mb) == self.minibatch:
                mb = pd.DataFrame(create_batch(self.current_regional_mb))
                self.on_regional_quote_mb(mb)

                self._current_regional_mb_index = None

    @events.after
    def on_regional_quote(self, quote):
        return {'type': 'level_1_regional_quote', 'data': quote}

    @events.after
    def on_regional_quote_mb(self, quote_list):
        return {'type': 'level_1_regional_quote_batch', 'data': quote_list}

    def regional_quote_provider(self):
        return IQFeedDataProvider(self.on_regional_quote_mb)

    def process_summary(self, summary: np.array):
        if self.fire_ticks:
            self.on_summary(iqfeed_to_dict(summary, self.key_suffix))

    @events.after
    def on_summary(self, summary):
        return {'type': 'level_1_tick', 'data': summary}

    def summary_provider(self):
        return IQFeedDataProvider(self.on_summary)

    def process_update(self, update: np.array):
        if self.fire_ticks:
            self.on_update(iqfeed_to_dict(update, self.key_suffix))

        if self.minibatch is not None:
            if self.current_update_mb is None:
                self.current_update_mb = np.empty((self.minibatch,), dtype=self.conn._update_dtype)
                self._current_update_mb_index = 0

            self.current_update_mb[self._current_update_mb_index] = update
            self._current_update_mb_index += 1

            if len(self.current_update_mb) == self.minibatch:
                mb = pd.DataFrame(create_batch(self.current_update_mb))
                self.on_update_mb(mb)
                self.current_update_mb = None

    @events.after
    def on_update(self, update):
        return {'type': 'level_1_tick', 'data': update}

    @events.after
    def on_update_mb(self, updates_list):
        return {'type': 'level_1_tick_batch', 'data': updates_list}

    def update_provider(self):
        return IQFeedDataProvider(self.on_update_mb)

    def process_fundamentals(self, fund: np.array):
        f = iqfeed_to_dict(fund, self.key_suffix)
        symbol = f['symbol']

        if symbol in self.fundamentals and isinstance(self.fundamentals[symbol], threading.Event):
            self.fundamentals[symbol].set()

        self.fundamentals[symbol] = f

        self.on_fundamentals(f)

    @events.after
    def on_fundamentals(self, fund: np.array):
        return {'type': 'level_1_fundamentals', 'data': fund}

    def fundamentals_provider(self):
        return IQFeedDataProvider(self.on_fundamentals)

    def get_fundamentals(self, symbol: typing.Union[set, str]):
        if isinstance(symbol, str):
            symbol = {symbol}

        fund_events = dict()

        with self._lock:
            try:
                for s in symbol:
                    if s not in self.fundamentals:
                        e = threading.Event()
                        self.fundamentals[s] = e
                        fund_events[s] = e
                        self.conn.watch(s)
            finally:
                for i, (k, e) in enumerate(fund_events.items()):
                    e.wait()
                    self.conn.unwatch(k)
                    if i % 20 == 0 and i > 0:
                        logging.getLogger(__name__).info("Fundamental data for " + str(i) + "/" + str(len(symbol)) + " symbols downloaded")

        if len(symbol) == 1:
            return self.fundamentals[next(iter(symbol))]
        else:
            return {s: self.fundamentals[s] for s in symbol}


def get_fundamentals(symbol: typing.Union[set, str], conn: iq.QuoteConn=None):
    with IQFeedLevel1Listener(fire_ticks=False, conn=conn) as listener:
        return listener.get_fundamentals(symbol)
