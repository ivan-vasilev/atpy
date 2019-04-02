import logging
import queue
import threading
import typing

import numpy as np
import pandas as pd

import pyiqfeed as iq
from atpy.data.iqfeed.util import launch_service, iqfeed_to_dict, IQFeedDataProvider
from pyevents.events import SyncListeners, EventFilter


class IQFeedLevel1Listener(iq.SilentQuoteListener):

    def __init__(self, listeners, fire_ticks=True, conn: iq.QuoteConn = None, key_suffix=''):
        super().__init__(name="Level 1 listener")

        self.listeners = listeners
        self.listeners += self.on_event

        self.fire_ticks = fire_ticks
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

    def on_event(self, event):
        if event['type'] == 'watch_ticks':
            self.watch(event['data'])

    def watch(self, symbol):
        with self._lock:
            if symbol not in self.fundamentals:
                self.conn.watch(symbol)

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        """
        You made a subscription request with an invalid symbol
        :param bad_symbol: The bad symbol
        """

        logging.getLogger(__name__).warning("Invalid symbol request: " + str(bad_symbol))

        if bad_symbol in self.fundamentals:
            if isinstance(self.fundamentals[bad_symbol], threading.Event):
                e = self.fundamentals[bad_symbol]
                del self.fundamentals[bad_symbol]
                e.set()

    def process_news(self, news_item: iq.QuoteConn.NewsMsg):
        news_item = news_item._asdict()

        self.listeners({'type': 'level_1_news_item', 'data': news_item})

        if self.minibatch is not None:
            self.current_news_mb.append(news_item)
            if len(self.current_news_mb) == self.minibatch:
                processed_data = {f + self.key_suffix: list() for f in news_item.keys()}
                for ni in self.current_news_mb:
                    for k, v in ni.items():
                        processed_data[k].append(v)

                self.listeners({'type': 'level_1_news_batch', 'data': processed_data})

                self.current_news_mb = list()

    def news_provider(self):
        return IQFeedDataProvider(listeners=self.listeners, accept_event=lambda e: True if e['type'] == 'level_1_news_item' else False)

    def process_regional_quote(self, quote: np.array):
        if self.fire_ticks:
            self.listeners({'type': 'level_1_regional_quote', 'data': iqfeed_to_dict(quote, self.key_suffix)})

    def regional_quote_provider(self):
        return IQFeedDataProvider(listeners=self.listeners, accept_event=lambda e: True if e['type'] == 'level_1_regional_quote' else False)

    def process_summary(self, summary: np.array):
        if self.fire_ticks:
            self.listeners({'type': 'level_1_tick', 'data': iqfeed_to_dict(summary, self.key_suffix)})

    def summary_provider(self):
        return IQFeedDataProvider(listeners=self.listeners, accept_event=lambda e: True if e['type'] == 'level_1_tick' else False)

    def process_update(self, update: np.array):
        if self.fire_ticks:
            self.listeners({'type': 'level_1_tick', 'data': iqfeed_to_dict(update, self.key_suffix)})

    def tick_event_filter(self):
        if not self.fire_ticks:
            raise AttributeError("This listener is not configured to fire ticks")

        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if 'type' in e and e['type'] == 'level_1_tick' else False,
                           event_transformer=lambda e: e['data'])

    def update_provider(self):
        return IQFeedDataProvider(listeners=self.listeners, accept_event=lambda e: True if e['type'] == 'level_1_tick_batch' else False)

    def process_fundamentals(self, fund: np.array):
        f = iqfeed_to_dict(fund, self.key_suffix)
        symbol = f['symbol']

        if symbol in self.fundamentals and isinstance(self.fundamentals[symbol], threading.Event):
            e = self.fundamentals[symbol]
            self.fundamentals[symbol] = f
            e.set()
            self.conn.unwatch(symbol)
        else:
            self.fundamentals[symbol] = f

        self.listeners({'type': 'level_1_fundamentals', 'data': f})

    def fundamentals_provider(self):
        return IQFeedDataProvider(listeners=self.listeners, accept_event=lambda e: True if e['type'] == 'level_1_fundamentals' else False)

    def get_fundamentals(self, symbol: typing.Union[set, str]):
        if isinstance(symbol, str):
            symbol = {symbol}

        with self._lock:
            batch = list()
            for i, s in enumerate(symbol):
                if s not in self.fundamentals:
                    e = threading.Event()
                    self.fundamentals[s] = e
                    batch.append((s, e))

                    if len(batch) == 10 or i == len(symbol) - 1:
                        for batch_sym, _ in batch:
                            self.conn.watch(batch_sym)

                        for _, e in batch:
                            e.wait()

                        batch = list()

                    if i % 100 == 0 and i > 0:
                        logging.getLogger(__name__).info("Fundamentals loaded: " + str(i))

        return {s: self.fundamentals[s] for s in symbol if s in self.fundamentals}


def get_fundamentals(symbol: typing.Union[set, str], conn: iq.QuoteConn = None):
    with IQFeedLevel1Listener(listeners=SyncListeners(), fire_ticks=False, conn=conn) as listener:
        return listener.get_fundamentals(symbol)


def get_splits_dividends(symbol: typing.Union[set, str], conn: iq.QuoteConn = None):
    funds = get_fundamentals(symbol=symbol, conn=conn)

    points = {'timestamp': list(), 'symbol': list(), 'type': list(), 'value': list()}
    for f in funds.values():
        if f['split_factor_1_date'] is not None and f['split_factor_1'] is not None:
            points['timestamp'].append(f['split_factor_1_date'])
            points['symbol'].append(f['symbol'])
            points['type'].append('split')
            points['value'].append(f['split_factor_1'])

        if f['split_factor_2_date'] is not None and f['split_factor_2'] is not None:
            points['timestamp'].append(f['split_factor_2_date'])
            points['symbol'].append(f['symbol'])
            points['type'].append('split')
            points['value'].append(f['split_factor_2'])

        if f['ex-dividend_date'] is not None and f['dividend_amount'] is not None:
            points['timestamp'].append(f['ex-dividend_date'])
            points['symbol'].append(f['symbol'])
            points['type'].append('dividend')
            points['value'].append(f['dividend_amount'])

    result = pd.DataFrame(points)

    if not result.empty:
        result['provider'] = 'iqfeed'

        result['timestamp'] = pd.to_datetime(result['timestamp'])
        result = result.set_index(['timestamp', 'symbol', 'type', 'provider'])
        result = result.tz_localize('US/Eastern', level=0).tz_convert('UTC', level=0)
        result = result[~result.index.duplicated(keep='last')]
        result.sort_index(inplace=True)

    return result
