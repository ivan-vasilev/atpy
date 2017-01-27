import queue

import numpy as np

import atpy.data.iqfeed.util as iqfeedutil
import pyiqfeed as iq
from atpy.data.iqfeed.data_events import *
from atpy.util.events_util import *


class IQFeedBarDataListener(iq.SilentBarListener):

    def __init__(self, minibatch=None, key_suffix='', column_mode=True, default_listeners=None):
        """
        :param minibatch: size of the minibatch
        :param key_suffix: suffix to the fieldnames
        :param column_mode: column/row data format
        :param default_listeners: list of the default listeners to be attached ot the event producers
        """
        super().__init__(name="Bar data listener")

        self.minibatch = minibatch
        self.conn = None
        self.key_suffix = key_suffix
        self.column_mode = column_mode
        self.current_batch = None

        default_listeners = default_listeners if default_listeners is not None else GlobalListeners()
        self.process_bar += default_listeners
        self.process_bar_batch += default_listeners

    def __enter__(self):
        iqfeedutil.launch_service()

        self.conn = iq.BarConn()
        self.conn.add_listener(self)
        self.conn.connect()

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

    def process_latest_bar_update(self, bar_data: np.array) -> None:
        self.process_bar(bar_data)

    def process_live_bar(self, bar_data: np.array) -> None:
        self.process_bar(bar_data)

    def process_history_bar(self, bar_data: np.array) -> None:
        self.process_bar(bar_data)

    @after
    def process_bar(self, bar_data):
        if self.minibatch is not None:
            if self.current_batch is None:
                self.current_batch = list()

            self.current_batch.append(bar_data[0] if len(bar_data) == 1 else bar_data)

            if len(self.current_batch) == self.minibatch:
                self.process_bar_batch(self.current_batch)
                self.current_batch = None

        return BarEvent({n + self.key_suffix: d for n, d in zip(bar_data.dtype.names, bar_data[0])})

    @after
    def process_bar_batch(self, batch):
        return BarBatchEvent(iqfeedutil.create_batch(batch, self.column_mode, self.key_suffix))


class IQFeedBarDataProvider(IQFeedBarDataListener):

    def __init__(self, minibatch=None, key_suffix='', column_mode=True, default_listeners=None):
        super().__init__(minibatch=minibatch, key_suffix=key_suffix, column_mode=column_mode, default_listeners=default_listeners)
        self.process_bar_batch += lambda event: self.queue.put(event.data)

    def __enter__(self):
        super().__enter__()
        self.queue = queue.Queue()

        return self

    def __iter__(self):
        return self

    def __next__(self):
        return self.queue.get()
