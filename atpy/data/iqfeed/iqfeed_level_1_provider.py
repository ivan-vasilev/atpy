from atpy.data.iqfeed.iqfeed_base_provider import *
from pyiqfeed import *
import queue
import numpy as np


class IQFeedLevel1Provider(IQFeedBaseProvider, SilentQuoteListener):

    def __init__(self, minibatch=1, key_suffix=''):
        super().__init__(name="data provider listener")

        self.minibatch = minibatch
        self.conn = None
        self.key_suffix = key_suffix

    def __iter__(self):
        super().__iter__()

        if self.conn is None:
            self.conn = iq.QuoteConn()
            self.conn.add_listener(self)
            self.conn.connect()

            self.queue = queue.Queue()

        return self

    def __enter__(self):
        super().__enter__()

        self.conn = iq.QuoteConn()
        self.conn.add_listener(self)
        self.conn.connect()
        self.conn.news_on()

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

    def __next__(self) -> map:
        result = None

        for i, datum in enumerate(iter(self.queue.get, None)):
            if result is None:
                result = {f + self.key_suffix: list() for f in datum._fields}

            for j, f in enumerate(datum._fields):
                result[f + self.key_suffix].append(datum[j])

            if (i + 1) % self.minibatch == 0:
                return result

    def __getattr__(self, name):
        if self.conn is not None:
            return getattr(self.conn, name)
        else:
            raise AttributeError

    def process_news(self, news_item: QuoteConn.NewsMsg) -> None:
        self.queue.put(news_item)

    def process_regional_quote(self, quote: np.array) -> None:
        """
        The top of book at a market-center was updated

        :param quote: numpy structured array with the actual quote

        dtype of quote is QuoteConn.regional_type

        """
        pass

    def process_summary(self, summary: np.array) -> None:
        """
        Initial data after subscription with latest quote, last trade etc.

        :param summary: numpy structured array with the data.

        Fields in each update can be changed by calling
        select_update_fieldnames on the QuoteConn class sending updates.

        The dtype of the array includes all requested fields. It can be
        different for each QuoteConn depending on the last call to
        select_update_fieldnames.

        """
        pass

    def process_update(self, update: np.array) -> None:
        """
        Update with latest quote, last trade etc.

        :param update: numpy structured array with the data.

        Compare with prior cached values to find our what changed. Nothing may
        have changed.

        Fields in each update can be changed by calling
        select_update_fieldnames on the QuoteConn class sending updates.

        The dtype of the array includes all requested fields. It can be
        different for each QuoteConn depending on the last call to
        select_update_fieldnames.

        """
        pass

    def process_fundamentals(self, fund: np.array) -> None:
        """
        Message with information about symbol which does not change.

        :param fund: numpy structured array with the data.

        Despite the word fundamentals used to describe this message in the
        IQFeed docs and the name of this function, you don't get just
        fundamental data. You also get reference date like the expiration date
        of an option.

        Called once when you first subscribe and every time you request a
        refresh.

        """
        pass
