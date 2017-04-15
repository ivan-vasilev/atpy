import collections

import pyevents.events as events
from atpy.data.iqfeed.util import *
import pandas as pd


class IQFeedBarDataListener(iq.SilentBarListener, metaclass=events.GlobalRegister):

    def __init__(self, fire_bars=True, minibatch=None, key_suffix=''):
        """
        :param fire_bars: raise event for each individual bar if True
        :param minibatch: size of the minibatch
        :param key_suffix: suffix to the fieldnames
        """
        super().__init__(name="Bar data listener")

        self.fire_bars = fire_bars
        self.minibatch = minibatch
        self.conn = None
        self.key_suffix = key_suffix
        self.current_batch = None

    def __enter__(self):
        launch_service()

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
        if self.fire_bars:
            self.on_latest_bar_update(self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix)))

    def process_live_bar(self, bar_data: np.array) -> None:
        self._on_bar(bar_data)

    def process_history_bar(self, bar_data: np.array) -> None:
        self._on_bar(bar_data)

    def _on_bar(self, bar_data):
        bar_data = np.copy(bar_data)

        if self.fire_bars:
            self.on_bar(self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix)))

        if self.minibatch is not None:
            if self.current_batch is None:
                self.current_batch = list()

            self.current_batch.append(bar_data)

            if len(self.current_batch) == self.minibatch:
                self.on_bar_batch(pd.DataFrame.from_dict(self._process_data(create_batch(self.current_batch, self.key_suffix))))
                self.current_batch = None

    def _process_data(self, data):
        if isinstance(data, dict):
            result = dict()

            result['Symbol'] = data.pop('symbol')
            result['Time Stamp'] = data.pop('date') + data.pop('time')
            result['High'] = data.pop('high_p')
            result['Low'] = data.pop('low_p')
            result['Open'] = data.pop('open_p')
            result['Close'] = data.pop('close_p')
            result['Total Volume'] = data.pop('tot_vlm')
            result['Period Volume'] = data.pop('prd_vlm')
            result['Number of Trades'] = data.pop('num_trds')
        elif isinstance(data, collections.Iterable):
            result = list()
            for d in data:
                result.append(self._process_data(d))

        return result

    @events.after
    def on_latest_bar_update(self, bar_data):
        return {'type': 'latest_bar_update', 'data': bar_data}

    @events.after
    def on_bar(self, bar_data):
        return {'type': 'bar', 'data': bar_data}

    def bar_provider(self):
        return IQFeedDataProvider(self.on_bar)

    @events.after
    def on_bar_batch(self, batch):
        return {'type': 'bar_batch', 'data': batch}

    def bar_batch_provider(self):
        return IQFeedDataProvider(self.on_bar_batch)
