import datetime
from atpy.data.iqfeed.iqfeed_base_provider import *
from typing import List
from atpy.data.iqfeed.filters import *
from pyevents.events import *


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


# class IQFeedNewsProvider(object):
#     """
#     IQFeed news data provider (not streaming). See the unit test on how to use
#     """
#
#     def __init__(self, minibatch=1, attach_text=False, random_order=False, key_suffix='', filter_provider=DefaultNewsFilterProvider(), column_mode=True):
#         self.minibatch = minibatch
#         self.attach_text = attach_text
#         self.conn = None
#         self.random_order = random_order
#         self.key_suffix = key_suffix
#         self.filter_provider = filter_provider
#         self.column_mode = True
#
#     def __iter__(self):
#         if self.conn is None:
#             self.conn = iq.NewsConn()
#             self.conn.connect()
#             self.cfg = self.conn.request_news_config()
#
#         return self
#
#     def __enter__(self):
#         self.conn = iq.NewsConn()
#         self.conn.connect()
#         self.cfg = self.conn.request_news_config()
#
#         self.queue = PCQueue(self.produce)
#         self.queue.start()
#
#         return self
#
#     def __exit__(self, exception_type, exception_value, traceback):
#         """Disconnect connection etc"""
#         self.queue.stop()
#         self.conn.disconnect()
#         self.quote_conn = None
#
#     def __del__(self):
#         if self.conn is not None:
#             self.conn.disconnect()
#             self.cfg = None
#
#         if self.queue is not None:
#             self.queue.stop()
#
#     def __next__(self) -> map:
#         result = {'id' + self.key_suffix: list(), 'headline' + self.key_suffix: list(),
#                   'date' + self.key_suffix: list(), 'time' + self.key_suffix: list(),
#                   'symbols' + self.key_suffix: list(), 'text' + self.key_suffix: list()}
#
#         for i, (story_id, headline, date, time, symbols, text) in enumerate(iter(self.queue.get, None)):
#             result['id' + self.key_suffix].append(story_id)
#             result['headline' + self.key_suffix].append(headline)
#             result['date' + self.key_suffix].append(date)
#             result['time' + self.key_suffix].append(time)
#             result['symbols' + self.key_suffix].append(symbols)
#
#             if text is not None:
#                 result['text' + self.key_suffix].append(text)
#
#             if (i + 1) % self.minibatch == 0:
#                 return result
#
#     def __getattr__(self, name):
#         if self.conn is not None:
#             return getattr(self.conn, name)
#         else:
#             raise AttributeError
#
#     def produce(self):
#         for f in self.filter_provider:
#             ids = list()
#             titles = list()
#
#             headlines = self.conn.request_news_headlines(sources=f.sources, symbols=f.symbols, date=f.date,
#                                                          limit=f.limit, timeout=f.timeout)
#
#             processed_headlines = list()
#             if self.attach_text:
#                 for h in headlines:
#                     if h.story_id not in ids and h.headline not in titles:
#                         story = self.conn.request_news_story(h.story_id)
#                         ids.append(h.story_id)
#                         titles.append(h.headline)
#
#                         processed_headlines.append((h.story_id, h.headline, h.story_date, h.story_time, h.symbol_list, story.story))
#             else:
#                 for h in headlines:
#                     if h.story_id not in ids and h.headline not in titles:
#                         ids.append(h.story_id)
#                         titles.append(h.headline)
#
#                         processed_headlines.append((h.story_id, h.headline, h.story_date, h.story_time, h.symbol_list, None))


class IQFeedNewsListener(object):
    """
    IQFeed news listener (not streaming). See the unit test on how to use
    """

    def __init__(self, minibatch=1, attach_text=False, random_order=False, key_suffix='', filter_provider=DefaultNewsFilterProvider(), column_mode=True):
        self.minibatch = minibatch
        self.attach_text = attach_text
        self.conn = None
        self.random_order = random_order
        self.key_suffix = key_suffix
        self.filter_provider = filter_provider
        self.column_mode = column_mode
        self.current_minibatch = None

    def __enter__(self):
        launch_service()

        self.conn = iq.NewsConn()
        self.conn.connect()
        self.cfg = self.conn.request_news_config()

        self.is_running = True
        self.producer_thread = threading.Thread(target=self.produce, daemon=True)
        self.producer_thread.start()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.conn.disconnect()
        self.quote_conn = None
        self.is_running = False

    def __del__(self):
        if self.conn is not None:
            self.conn.disconnect()
            self.cfg = None

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

            processed_data = None

            for h in headlines:
                if h.story_id not in ids and h.headline not in titles:
                    ids.append(h.story_id)
                    titles.append(h.headline)

                    h = h._asdict()

                    if self.attach_text:
                        h['text'] = self.conn.request_news_story(h['story_id'])

                    if self.column_mode:
                        if processed_data is None:
                            processed_data = {f + self.key_suffix: list() for f in h.keys()}

                        if self.current_minibatch is None:
                            self.current_minibatch = {f + self.key_suffix: list() for f in h.keys()}

                        for key, value in h.items():
                            self.current_minibatch[key + self.key_suffix].append(value)
                            processed_data[key + self.key_suffix].append(value)

                        if len(self.current_minibatch[list(self.current_minibatch.keys())[0]]) == self.minibatch:
                            self.process_minibatch(self.current_minibatch)
                            self.current_minibatch = None
                    else:
                        if processed_data is None:
                            processed_data = list()

                        if self.current_minibatch is None:
                            self.current_minibatch = list()

                        h = {k + self.key_suffix: v for k, v in h.items()}

                        processed_data.append(h)

                        self.current_minibatch.append(h)
                        processed_data.append(h)

                        if len(self.current_minibatch) == self.minibatch:
                            self.process_minibatch(self.current_minibatch)
                            self.current_minibatch = list()

            self.process_batch(processed_data)

            if not self.is_running:
                break

    @after
    def process_batch(self, data):
        return data

    @after
    def process_minibatch(self, data):
        return data


class IQFeedMewsProvider(IQFeedNewsListener):
    """
    IQFeed historical data provider (not streaming). See the unit test on how to use
    """

    def __init__(self, minibatch=1, column_mode=True, attach_text=True, key_suffix='', filter_provider=DefaultNewsFilterProvider(), use_minibatch=True):
        """
        :param minibatch: size of the minibatch
        :param column_mode: whether to organize the data in columns or rows
        :param attach_text: attach text (otherwise, only headline is attached
        :param key_suffix: suffix for field names
        :param filter_provider: news filter list
        :param use_minibatch: minibatch vs full patch
        """
        super().__init__(minibatch=minibatch, attach_text=attach_text, key_suffix=key_suffix, column_mode=column_mode, filter_provider=filter_provider)

        if use_minibatch:
            self.process_minibatch += lambda *args, **kwargs: self.queue.put(kwargs[FUNCTION_OUTPUT])
        else:
            self.process_batch += lambda *args, **kwargs: self.queue.put(kwargs[FUNCTION_OUTPUT])

    def __enter__(self):
        self.queue = queue.Queue()
        super().__enter__()
        return self

    def __iter__(self):
        return self

    def __next__(self) -> map:
        return self.queue.get()
