from atpy.data.iqfeed.iqfeed_base_provider import *
from pyiqfeed import *
import queue


class IQFeedLevel2Provider(IQFeedBaseProvider, SilentQuoteListener):

    def __init__(self, minibatch=1, key_suffix=''):
        super().__init__(name="data provider listener")

        self.minibatch = minibatch
        self.quote_conn = None
        self.key_suffix = key_suffix

    def __iter__(self):
        super().__iter__()

        if self.quote_conn is None:
            self.quote_conn = iq.QuoteConn()
            self.quote_conn.add_listener(self)
            self.quote_conn.connect()
            self.quote_conn.news_on()

        self.queue = queue.Queue()

        return self

    def __enter__(self):
        super().__enter__()

        self.quote_conn = iq.QuoteConn()
        self.quote_conn.add_listener(self)
        self.quote_conn.connect()
        self.quote_conn.news_on()

        self.queue = queue.Queue()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.quote_conn.remove_listener(self)
        self.quote_conn.disconnect()
        self.quote_conn = None

    def __del__(self):
        if self.quote_conn is not None:
            self.quote_conn.remove_listener(self)
            self.quote_conn.disconnect()

    def __next__(self) -> map:
        result = None

        for i, datum in enumerate(iter(self.queue.get, None)):
            if result is None:
                result = {f + self.key_suffix: list() for f in datum._fields}

            for j, f in enumerate(datum._fields):
                result[f + self.key_suffix].append(datum[j])

            if (i + 1) % self.minibatch == 0:
                return result

    def process_news(self, news_item: QuoteConn.NewsMsg) -> None:
        self.queue.put(news_item)
