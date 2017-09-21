import queue
import threading
from typing import Sequence

import numpy as np
import pandas as pd

import pyevents.events as events
import pyiqfeed as iq
from atpy.data.iqfeed.util import launch_service, iqfeed_to_dict, create_batch, IQFeedDataProvider


class IQFeedLevel1Listener(iq.SilentQuoteListener, metaclass=events.GlobalRegister):

    def __init__(self, fire_ticks=True, minibatch=None, key_suffix=''):
        super().__init__(name="Level 1 listener")

        self.fire_ticks = fire_ticks
        self.minibatch = minibatch
        self.conn = None
        self.key_suffix = key_suffix

        self.watched_symbols = set()

        self.current_fund_mb = list()
        self.current_news_mb = list()
        self.current_regional_mb = None
        self.current_update_mb = None

    def __enter__(self):
        launch_service()

        self.conn = iq.QuoteConn()
        self.conn.add_listener(self)
        self.conn.connect()

        self.queue = queue.Queue()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.conn.remove_listener(self)
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
            if event['data'] not in self.watched_symbols:
                self.conn.watch(event['data'])
                self.watched_symbols.add(event['data'])

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        """
        You made a subscription request with an invalid symbol

        :param bad_symbol: The bad symbol

        """
        if bad_symbol in self.watched_symbols and bad_symbol in self.watched_symbols:
            self.watched_symbols.remove(bad_symbol)

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
        return IQFeedDataProvider(self.on_news_mb)

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

        Fundamentals.fundamentals[f['symbol']] = f

        self.on_fundamentals(f)

    @events.after
    def on_fundamentals(self, fund: np.array):
        return {'type': 'level_1_fundamentals', 'data': fund}

    def fundamentals_provider(self):
        return IQFeedDataProvider(self.on_fundamentals)


class Fundamentals(iq.SilentQuoteListener):

    fundamentals = dict()
    lock = threading.RLock()

    @staticmethod
    def get(symbol: str, conn: iq.QuoteConn=None):
        if symbol not in Fundamentals.fundamentals:
            if not hasattr(Fundamentals, 'threading_events'):
                Fundamentals.threading_events = dict()

            Fundamentals.threading_events[symbol] = threading.Event()

            class FL(iq.SilentQuoteListener):
                def __init__(self):
                    super().__init__("fundamental_listener")

                def process_fundamentals(self, fund: np.array):
                    with Fundamentals.lock:
                        Fundamentals.fundamentals[symbol] = iqfeed_to_dict(fund)
                        if symbol in Fundamentals.threading_events:
                            Fundamentals.threading_events[symbol].set()

            fl = FL()

            try:
                if conn is None:
                    _conn = iq.QuoteConn()
                    _conn.connect()
                else:
                    _conn = conn

                _conn.add_listener(fl)
                _conn.watch(symbol)
                Fundamentals.threading_events[symbol].wait()
            finally:
                del Fundamentals.threading_events[symbol]
                _conn.remove_listener(fl)
                _conn.unwatch(symbol)

                if conn is None:
                    _conn.disconnect()

        return Fundamentals.fundamentals[symbol]
