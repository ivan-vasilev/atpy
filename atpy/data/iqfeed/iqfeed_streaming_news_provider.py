from atpy.data.iqfeed.iqfeed_base_provider import *
from pyiqfeed import *
import queue


class IQFeedStreamingNewsProvider(IQFeedBaseProvider, SilentQuoteListener):
    def __init__(self, minibatch=1, key_suffix=''):
        self.minibatch = minibatch
        self.quote_conn = None
        self.key_suffix = key_suffix

    def __iter__(self):
        super().__iter__()

        if self.quote_conn is None or not self.quote_conn.connected():
            self.quote_conn = iq.QuoteConn()
            self.quote_conn.add_listener(self)
            self.quote_conn.add_listener(VerboseQuoteListener("test_listener"))
            self.quote_conn.connect()
            self.quote_conn.news_on()

        self.queue = queue.Queue()

        return self

    def process_news(self, news_item: QuoteConn.NewsMsg) -> None:
        self.queue.put(news_item.story_id, news_item.headline, news_item.story_time, news_item.symbol_list,
                       news_item.distributor)

    def __del__(self):
        if self.quote_conn is not None and self.quote_conn.connected():
            self.quote_conn.disconnect()

    def __next__(self) -> map:
        result = {'id' + self.key_suffix: list(), 'headline' + self.key_suffix: list(),
                  'time' + self.key_suffix: list(),
                  'symbols' + self.key_suffix: list(),
                  'distributor' + self.key_suffix: list()}

        for i, (story_id, headline, date, time, symbols, distributor) in enumerate(iter(self.queue.get, None)):
            result['id' + self.key_suffix].append(story_id)
            result['headline' + self.key_suffix].append(headline)
            result['time' + self.key_suffix].append(time)
            result['symbols' + self.key_suffix].append(symbols)
            result['distributor' + self.key_suffix].append(distributor)

            if (i + 1) % self.minibatch == 0:
                return result
