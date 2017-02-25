from atpy.data.iqfeed.util import *
from pyevents.events import *
from typing import Sequence


class IQFeedLevel1Listener(iq.SilentQuoteListener):
    def __init__(self, minibatch=None, key_suffix='', column_mode=True, default_listeners=None):
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

        if default_listeners is not None:
            self.process_update += default_listeners
            self.process_update_mb += default_listeners
            self.process_news += default_listeners
            self.process_news_mb += default_listeners
            self.process_summary += default_listeners
            self.process_summary_mb += default_listeners
            self.process_regional_quote += default_listeners
            self.process_regional_mb += default_listeners
            self.process_fundamentals += default_listeners
            self.process_fundamentals_mb += default_listeners
            default_listeners += self.on_event

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

    def on_event(self, event):
        if event['type'] == 'watch_symbol':
            if event['symbol'] not in self.watched_symbols:
                self.conn.watch(event['symbol'])
                self.watched_symbols.add(event['symbol'])
        elif event['type'] == 'portfolio_update':
            to_watch = event['data'].symbols - self.watched_symbols
            for symbol in to_watch:
                self.conn.watch(symbol)
                self.watched_symbols.add(symbol)

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        """
        You made a subscription request with an invalid symbol

        :param bad_symbol: The bad symbol

        """
        if bad_symbol in self.watched_symbols and bad_symbol in self.watched_symbols:
            self.watched_symbols.remove(bad_symbol)

    @after
    def process_news(self, news_item: iq.QuoteConn.NewsMsg):
        news_item = news_item._asdict()

        if self.minibatch is not None:
            self.current_news_mb.append(news_item)
            if len(self.current_news_mb) == self.minibatch:
                if self.column_mode:
                    processed_data = {f + self.key_suffix: list() for f in news_item.keys()}
                    for ni in self.current_news_mb:
                        for k, v in ni.items():
                            processed_data[k].append(v)

                    self.process_news_mb(processed_data)
                else:
                    self.process_news_mb(self.current_news_mb)

                self.current_news_mb = list()

        return {'type': 'level_1_news_item', 'data': news_item}

    @after
    def process_news_mb(self, news_list):
        return {'type': 'level_1_news_batch', 'data': news_list}

    def news_provider(self):
        return IQFeedDataProvider(self.process_news_mb)

    def process_watched_symbols(self, symbols: Sequence[str]) -> None:
        """List of all watched symbols when requested."""
        self.watched_symbols = symbols

    @after
    def process_regional_quote(self, quote: np.array):
        if self.minibatch is not None:
            self.current_regional_mb.append(quote)
            if len(self.current_regional_mb) == self.minibatch:
                self.process_regional_mb(create_batch(self.current_regional_mb, self.column_mode, self.key_suffix))
                self.current_regional_mb = list()

        return {'type': 'level_1_regional_quote', 'data': iqfeed_to_dict(quote, self.key_suffix)}

    @after
    def process_regional_mb(self, quote_list):
        return {'type': 'level_1_regional_quote_batch', 'data': quote_list}

    def regional_provider(self):
        return IQFeedDataProvider(self.process_regional_mb)

    @after
    def process_summary(self, summary: np.array):
        if self.minibatch is not None:
            self.current_summary_mb.append(summary)
            if len(self.current_summary_mb) == self.minibatch:
                self.process_summary_mb(create_batch(self.current_summary_mb, self.column_mode, self.key_suffix))
                self.current_summary_mb = list()

        return {'type': 'level_1_tick', 'data': iqfeed_to_dict(summary, self.key_suffix)}

    @after
    def process_summary_mb(self, summary_list):
        return {'type': 'level_1_tick_batch', 'data': summary_list}

    def summary_provider(self):
        return IQFeedDataProvider(self.process_summary_mb)

    @after
    def process_update(self, update: np.array):
        if self.minibatch is not None:
            self.current_update_mb.append(update)
            if len(self.current_update_mb) == self.minibatch:
                self.process_update_mb(create_batch(self.current_update_mb, self.column_mode, self.key_suffix))
                self.current_update_mb = list()

        return {'type': 'level_1_tick', 'data': iqfeed_to_dict(update, self.key_suffix)}

    @after
    def process_update_mb(self, updates_list):
        return {'type': 'level_1_tick_batch', 'data': updates_list}

    def update_provider(self):
        return IQFeedDataProvider(self.process_update_mb)

    @after
    def process_fundamentals(self, fund: np.array):
        if self.minibatch is not None:
            self.current_fund_mb.append(fund)
            if len(self.current_fund_mb) == self.minibatch:
                self.process_fundamentals_mb(create_batch(self.current_fund_mb, self.column_mode, self.key_suffix))
                self.current_fund_mb = list()

        return {'type': 'level_1_fundamentals', 'data': iqfeed_to_dict(fund, self.key_suffix)}

    @after
    def process_fundamentals_mb(self, fund_list):
        return {'type': 'level_1_fundamental_batch', 'data': fund_list}

    def fundamentals_provider(self):
        return IQFeedDataProvider(self.process_fundamentals_mb)
