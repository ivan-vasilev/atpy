from atpy.data.iqfeed.iqfeed_base_provider import *
from pyiqfeed import *
import queue
import numpy as np


class IQFeedBarDataProvider(IQFeedBaseProvider, SilentBarListener):

    def __init__(self, minibatch=1, key_suffix=''):
        super().__init__(name="data provider listener")

        self.minibatch = minibatch
        self.conn = None
        self.key_suffix = key_suffix

    def __iter__(self):
        super().__iter__()

        if self.conn is None:
            self.conn = iq.BarConn()
            self.conn.add_listener(self)
            self.conn.connect()

            self.queue = queue.Queue()

        return self

    def __enter__(self):
        super().__enter__()

        self.conn = iq.BarConn()
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

    def __next__(self) -> map:
        for i, datum in enumerate(iter(self.queue.get, None)):
            if i == 0:
                result = {n: np.empty((self.minibatch,), d.dtype) for n, d in zip(datum.dtype.names, datum[0])}

            for j, f in enumerate(datum.dtype.names):
                result[f][i] = datum[0][j]

            if (i + 1) % self.minibatch == 0:
                return result

    def __getattr__(self, name):
        if self.conn is not None:
            return getattr(self.conn, name)
        else:
            raise AttributeError

    def process_latest_bar_update(self, bar_data: np.array) -> None:
        self.queue.put(bar_data)

    def process_live_bar(self, bar_data: np.array) -> None:
        self.queue.put(bar_data)

    def process_history_bar(self, bar_data: np.array) -> None:
        self.queue.put(bar_data)
