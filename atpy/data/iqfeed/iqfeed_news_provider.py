import datetime
from atpy.data.iqfeed.iqfeed_base_provider import *
from typing import List
from atpy.data.iqfeed.filters import *


class NewsFilter(NamedTuple):
    """
    News filter parameters
    """
    sources: List[str]
    symbols: List[str]
    date: datetime.date
    timeout: int
    limit: int

NewsFilter.__new__.__defaults__ = (None, None, None, None, 100000)


class DefaultNewsFilterProvider(DefaultFilterProvider):
    """Default news filter provider, which contains a list of filters"""

    def _default_filter(self):
        return NewsFilter()


class IQFeedNewsProvider(IQFeedBaseProvider):
    """
    IQFeed news data provider (not streaming). See the unit test on how to use
    """

    def __init__(self, minibatch=1, attach_text=False, random_order=False, key_suffix='', filter_provider=DefaultNewsFilterProvider()):
        self.minibatch = minibatch
        self.attach_text = attach_text
        self.conn = None
        self.random_order = random_order
        self.key_suffix = key_suffix
        self.filter_provider = filter_provider

    def __iter__(self):
        super().__iter__()

        if self.conn is None:
            self.conn = iq.NewsConn()
            self.conn.connect()
            self.cfg = self.conn.request_news_config()

        return self

    def __enter__(self):
        super().__enter__()

        self.conn = iq.NewsConn()
        self.conn.connect()
        self.cfg = self.conn.request_news_config()

        self.queue = PCQueue(self.produce)
        self.queue.start()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.queue.stop()
        self.conn.disconnect()
        self.quote_conn = None

    def __del__(self):
        if self.conn is not None:
            self.conn.disconnect()
            self.cfg = None

        if self.queue is not None:
            self.queue.stop()

    def __next__(self) -> map:
        result = {'id' + self.key_suffix: list(), 'headline' + self.key_suffix: list(),
                  'date' + self.key_suffix: list(), 'time' + self.key_suffix: list(),
                  'symbols' + self.key_suffix: list(), 'text' + self.key_suffix: list()}

        for i, (story_id, headline, date, time, symbols, text) in enumerate(iter(self.queue.get, None)):
            result['id' + self.key_suffix].append(story_id)
            result['headline' + self.key_suffix].append(headline)
            result['date' + self.key_suffix].append(date)
            result['time' + self.key_suffix].append(time)
            result['symbols' + self.key_suffix].append(symbols)

            if text is not None:
                result['text' + self.key_suffix].append(text)

            if (i + 1) % self.minibatch == 0:
                return result

    def __getattr__(self, name):
        if self.conn is not None:
            return getattr(self.conn, name)
        else:
            raise AttributeError

    def produce(self):
        for f in self.filter_provider:
            ids = list()
            titles = list()

            headlines = self.conn.request_news_headlines(sources=f.sources, symbols=f.symbols, date=f.date,
                                                         limit=f.limit, timeout=f.timeout)
            if self.attach_text:
                for h in headlines:
                    if h.story_id not in ids and h.headline not in titles:
                        story = self.conn.request_news_story(h.story_id)
                        ids.append(h.story_id)
                        titles.append(h.headline)

                        yield h.story_id, h.headline, h.story_date, h.story_time, h.symbol_list, story.story
            else:
                for h in headlines:
                    if h.story_id not in ids and h.headline not in titles:
                        ids.append(h.story_id)
                        titles.append(h.headline)

                        yield h.story_id, h.headline, h.story_date, h.story_time, h.symbol_list, None
