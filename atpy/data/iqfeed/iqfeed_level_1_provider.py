import queue

import pyiqfeed as iq
from atpy.data.iqfeed.iqfeed_events import *
from atpy.data.iqfeed.util import *
from pyeventsml.events_util import *


class IQFeedLevel1Listener(iq.SilentQuoteListener):

    def __init__(self, minibatch=None, key_suffix='', column_mode=True, default_listeners=None):
        super().__init__(name="Level 1 listener")

        self.minibatch = minibatch
        self.conn = None
        self.key_suffix = key_suffix
        self.column_mode = column_mode

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

        return Level1NewsItemEvent(news_item)

    @after
    def process_news_mb(self, news_list):
        return Level1NewsBatchEvent(news_list)

    def news_provider(self):
        return StreamingDataProvider(self.process_news_mb)

    @after
    def process_regional_quote(self, quote: np.array):
        if self.minibatch is not None:
            self.current_regional_mb.append(quote)
            if len(self.current_regional_mb) == self.minibatch:
                self.process_regional_mb(create_batch(self.current_regional_mb, self.column_mode, self.key_suffix))
                self.current_regional_mb = list()

        return Level1RegionalQuoteEvent(iqfeed_to_dict(quote, self.key_suffix))

    @after
    def process_regional_mb(self, quote_list):
        return Level1RegionalQuoteBatchEvent(quote_list)

    def regional_provider(self):
        return StreamingDataProvider(self.process_regional_mb)

    @after
    def process_summary(self, summary: np.array):
        if self.minibatch is not None:
            self.current_summary_mb.append(summary)
            if len(self.current_summary_mb) == self.minibatch:
                self.process_summary_mb(create_batch(self.current_summary_mb, self.column_mode, self.key_suffix))
                self.current_summary_mb = list()

        return Level1SummaryEvent(iqfeed_to_dict(summary, self.key_suffix))

    @after
    def process_summary_mb(self, summary_list):
        return Level1SummaryBatchEvent(summary_list)

    def summary_provider(self):
        return StreamingDataProvider(self.process_summary_mb)

    @after
    def process_update(self, update: np.array):
        if self.minibatch is not None:
            self.current_update_mb.append(update)
            if len(self.current_update_mb) == self.minibatch:
                self.process_update_mb(create_batch(self.current_update_mb, self.column_mode, self.key_suffix))
                self.current_update_mb = list()

        return Level1UpdateEvent(iqfeed_to_dict(update, self.key_suffix))

    @after
    def process_update_mb(self, updates_list):
        return Level1UpdateBatchEvent(updates_list)

    def update_provider(self):
        return StreamingDataProvider(self.process_update_mb)

    @after
    def process_fundamentals(self, fund: np.array):
        if self.minibatch is not None:
            self.current_fund_mb.append(fund)
            if len(self.current_fund_mb) == self.minibatch:
                self.process_fundamentals_mb(create_batch(self.current_fund_mb, self.column_mode, self.key_suffix))
                self.current_fund_mb = list()

        return Level1FundamentalsEvent(iqfeed_to_dict(fund, self.key_suffix))

    @after
    def process_fundamentals_mb(self, fund_list):
        return Level1FundamentalsBatchEvent(fund_list)

    def fundamentals_provider(self):
        return StreamingDataProvider(self.process_fundamentals_mb)


class StreamingDataProvider(object):
    """Streaming data provider generator/iterator interface"""

    def __init__(self, producer):
        self.q = queue.Queue()
        producer += lambda event: self.q.put(event.data)

    def __iter__(self):
        return self

    def __next__(self):
        return self.q.get()
