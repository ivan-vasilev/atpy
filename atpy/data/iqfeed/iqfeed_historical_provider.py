import datetime
from atpy.data.iqfeed.iqfeed_base_provider import *
from typing import List, NamedTuple
from abc import *


class HistoryFilter(NamedTuple):
    """
    History filter parameters
    """
    sources: List[str]
    symbols: List[str]
    date: datetime.date
    timeout: int
    limit: int

HistoryFilter.__new__.__defaults__ = (None, None, None, None, 100000)


class HistoryFilterProvider(metaclass=ABCMeta):
    """Base news filter provider generator/iterator interface"""

    @abstractmethod
    def __iter__(self):
        return

    @abstractmethod
    def __next__(self) -> HistoryFilter:
        return


class DefaultFilterProvider(NewsFilterProvider):
    """Default news filter provider, which contains a list of filters"""

    def __init__(self):
        self._filters = list()

    def __iadd__(self, fn):
        self._filters.append(fn)
        return self

    def __isub__(self, fn):
        self._filters.remove(fn)
        return self

    def __iter__(self):
        self.__counter = -1
        return self

    def __next__(self) -> NewsFilter:
        if len(self._filters) == 0:
            return NewsFilter()
        else:
            self.__counter += 1
            return self._filters[self.__counter % len(self._filters)]


class IQFeedNewsProvider(IQFeedBaseProvider):
    """
    IQFeed news data provider (not streaming). Use like
    >>> news_provider = IQFeedNewsProvider(attach_text=True)
    >>> news_provider.add_filter(NewsFilter(symbols=['AAPL']))
    >>> news_provider.add_filter(NewsFilter(symbols=['IBM']))
    >>>
    >>> for i in news_provider:
    >>>     print(i)
    """

    def __init__(self, minibatch=1, attach_text=False, random_order=False, key_suffix='', filter_provider=DefaultFilterProvider()):
        self.minibatch = minibatch
        self.attach_text = attach_text
        self.news_conn = None
        self.random_order = random_order
        self.key_suffix = key_suffix
        self.filter_provider = filter_provider

    def __iter__(self):
        super().__iter__()

        if self.news_conn is None or not self.news_conn.connected():
            self.news_conn = iq.NewsConn()
            self.news_conn.connect()
            self.cfg = self.news_conn.request_news_config()

        self.queue = PCQueue(self.produce)
        self.queue.start()

        return self

    def produce(self):
        for f in self.filter_provider:
            ids = list()
            titles = list()

            headlines = self.news_conn.request_news_headlines(sources=f.sources, symbols=f.symbols, date=f.date,
                                                              limit=f.limit, timeout=f.timeout)
            if self.attach_text:
                for h in headlines:
                    if h.story_id not in ids and h.headline not in titles:
                        story = self.news_conn.request_news_story(h.story_id)
                        ids.append(h.story_id)
                        titles.append(h.headline)

                        yield h.story_id, h.headline, h.story_date, h.story_time, h.symbol_list, story.story
            else:
                for h in headlines:
                    if h.story_id not in ids and h.headline not in titles:
                        ids.append(h.story_id)
                        titles.append(h.headline)

                        yield h.story_id, h.headline, h.story_date, h.story_time, h.symbol_list, None

    def __del__(self):
        if self.news_conn is not None and self.news_conn.connected():
            self.news_conn.disconnect()
            self.cfg = None

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
