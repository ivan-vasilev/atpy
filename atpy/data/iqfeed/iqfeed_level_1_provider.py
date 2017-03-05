from atpy.data.iqfeed.util import *
import pyevents.events as events
from typing import Sequence


class IQFeedLevel1Listener(iq.SilentQuoteListener, metaclass=events.GlobalRegister):
    def __init__(self, minibatch=None, key_suffix='', column_mode=True):
        super().__init__(name="Level 1 listener")

        self.minibatch = minibatch
        self.conn = None
        self.key_suffix = key_suffix
        self.column_mode = column_mode

        self.watched_symbols = set()

        self.current_fund_mb = list()
        self.current_news_mb = list()
        self.current_regional_mb = list()
        self.current_summary_mb = list()
        self.current_update_mb = list()

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
        if event['type'] == 'watch_symbol':
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
        super().process_news(news_item)

        news_item = news_item._asdict()

        self.on_news(news_item)

        if self.minibatch is not None:
            self.current_news_mb.append(news_item)
            if len(self.current_news_mb) == self.minibatch:
                if self.column_mode:
                    processed_data = {f + self.key_suffix: list() for f in news_item.keys()}
                    for ni in self.current_news_mb:
                        for k, v in ni.items():
                            processed_data[k].append(v)

                    self.on_news_mb(processed_data)
                else:
                    self.on_news_mb(self.current_news_mb)

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
        super().process_watched_symbols(symbols)

        self.watched_symbols = symbols

    def process_regional_quote(self, quote: np.array):
        super().process_regional_quote(quote)

        self.on_regional_quote(iqfeed_to_dict(quote, self.key_suffix))

        if self.minibatch is not None:
            self.current_regional_mb.append(quote)
            if len(self.current_regional_mb) == self.minibatch:
                self.on_regional_mb(create_batch(self.current_regional_mb, self.column_mode, self.key_suffix))
                self.current_regional_mb = list()

    @events.after
    def on_regional_quote(self, quote):
        return {'type': 'level_1_regional_quote', 'data': quote}

    @events.after
    def on_regional_quote_mb(self, quote_list):
        return {'type': 'level_1_regional_quote_batch', 'data': quote_list}

    def regional_quote_provider(self):
        return IQFeedDataProvider(self.on_regional_quote_mb)

    def process_summary(self, summary: np.array):
        super().process_summary(summary)

        self.on_summary(iqfeed_to_dict(summary, self.key_suffix))

        if self.minibatch is not None:
            self.current_summary_mb.append(summary)
            if len(self.current_summary_mb) == self.minibatch:
                self.on_summary_mb(create_batch(self.current_summary_mb, self.column_mode, self.key_suffix))
                self.current_summary_mb = list()

    @events.after
    def on_summary(self, summary):
        return {'type': 'level_1_tick', 'data': summary}

    @events.after
    def on_summary_mb(self, summary_list):
        return {'type': 'level_1_tick_batch', 'data': summary_list}

    def summary_provider(self):
        return IQFeedDataProvider(self.on_summary_mb)

    def process_update(self, update: np.array):
        super().process_update(update)

        self.on_update(iqfeed_to_dict(update, self.key_suffix))

        if self.minibatch is not None:
            self.current_update_mb.append(update)
            if len(self.current_update_mb) == self.minibatch:
                self.on_update_mb(create_batch(self.current_update_mb, self.column_mode, self.key_suffix))
                self.current_update_mb = list()

    @events.after
    def on_update(self, update):
        return {'type': 'level_1_tick', 'data': update}

    @events.after
    def on_update_mb(self, updates_list):
        return {'type': 'level_1_tick_batch', 'data': updates_list}

    def update_provider(self):
        return IQFeedDataProvider(self.on_update_mb)

    def process_fundamentals(self, fund: np.array):
        super().process_fundamentals(fund)

        self.on_fundamentals(iqfeed_to_dict(fund, self.key_suffix))

        if self.minibatch is not None:
            self.current_fund_mb.append(fund)
            if len(self.current_fund_mb) == self.minibatch:
                self.on_fundamentals_mb(create_batch(self.current_fund_mb, self.column_mode, self.key_suffix))
                self.current_fund_mb = list()

    @events.after
    def on_fundamentals(self, fund: np.array):
        return {'type': 'level_1_fundamentals', 'data': fund}

    @events.after
    def on_fundamentals_mb(self, fund_list):
        return {'type': 'level_1_fundamental_batch', 'data': fund_list}

    def fundamentals_provider(self):
        return IQFeedDataProvider(self.on_fundamentals_mb)
