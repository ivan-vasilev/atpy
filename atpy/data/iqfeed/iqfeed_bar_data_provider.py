import atpy.data.iqfeed.util as iqfeedutil
from atpy.data.iqfeed.util import *
from pyevents.events import *


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

        if default_listeners is not None:
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

        return {'type': 'bar_data', 'data': {n + self.key_suffix: d for n, d in zip(bar_data.dtype.names, bar_data[0])}}

    def bar_provider(self):
        return IQFeedDataProvider(self.process_bar)

    @after
    def process_bar_batch(self, batch):
        return {'type': 'bar_batch_event', 'data': iqfeedutil.create_batch(batch, self.column_mode, self.key_suffix)}

    def bar_batch_provider(self):
        return IQFeedDataProvider(self.process_bar_batch)
