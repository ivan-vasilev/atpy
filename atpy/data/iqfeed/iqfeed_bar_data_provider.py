import pyevents.events as events
from atpy.data.iqfeed.util import *
from atpy.data.iqfeed.iqfeed_level_1_provider import Fundamentals
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
        self.watched_symbols = set()

    def __enter__(self):
        launch_service()

        self.conn = iq.BarConn()
        self.conn.add_listener(self)
        self.conn.connect()

        # streaming conn for fundamental data
        self.streaming_conn = iq.QuoteConn()
        self.streaming_conn.connect()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        self.conn.remove_listener(self)
        self.conn.disconnect()

        self.streaming_conn.disconnect()
        self.streaming_conn = None

        self.conn = None

    def __del__(self):
        if self.conn is not None:
            self.conn.remove_listener(self)
            self.conn.disconnect()

        if self.streaming_conn is not None:
            self.streaming_conn.disconnect()

    def __getattr__(self, name):
        if self.conn is not None:
            return getattr(self.conn, name)
        else:
            raise AttributeError

    def process_latest_bar_update(self, bar_data: np.array) -> None:
        if self.fire_bars:
            self.on_latest_bar_update(self._process_data(iqfeed_to_dict(np.copy(bar_data), key_suffix=self.key_suffix)))

    def process_live_bar(self, bar_data: np.array) -> None:
        self._on_bar(np.copy(bar_data)[0])
        if self.fire_bars:
            self.on_bar(self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix)))

    def process_history_bar(self, bar_data: np.array) -> None:
        bar_data = np.copy(bar_data)
        bd = bar_data[0] if len(bar_data) == 1 else bar_data
        adjust(bd, Fundamentals.get(bd['symbol'].decode('ascii'), self.streaming_conn))

        if self.fire_bars:
            self.on_bar(self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix)))

        self._on_bar(bd)

    @events.listener
    def on_event(self, event):
        if event['type'] == 'watch_bars':
            if event['data'] not in self.watched_symbols:
                self.conn.watch(event['data'])
                self.watched_symbols.add(event['data'])

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        if bad_symbol in self.watched_symbols and bad_symbol in self.watched_symbols:
            self.watched_symbols.remove(bad_symbol)

    def _on_bar(self, bar_data):
        if self.minibatch is not None:
            if self.current_batch is None:
                self.current_batch = np.empty((self.minibatch,), iq.BarConn.interval_data_type)
                self._current_mb_index = 0

            self.current_batch[self._current_mb_index] = bar_data
            self._current_mb_index += 1

            if self._current_mb_index == self.minibatch:
                bar_dataframe = self._process_data(self.current_batch)
                self.on_bar_batch(bar_dataframe)
                self.current_batch = None
                self._current_mb_index = None

    def _process_data(self, data):
        if isinstance(data, dict):
            result = dict()

            sf = self.key_suffix

            result['Symbol' + sf] = data.pop('symbol')
            result['Time Stamp' + sf] = data.pop('date') + data.pop('time')
            result['High' + sf] = data.pop('high_p')
            result['Low' + sf] = data.pop('low_p')
            result['Open' + sf] = data.pop('open_p')
            result['Close' + sf] = data.pop('close_p')
            result['Total Volume' + sf] = data.pop('tot_vlm')
            result['Period Volume' + sf] = data.pop('prd_vlm')
            result['Number of Trades' + sf] = data.pop('num_trds')
        else:
            result = pd.DataFrame(data)
            result['symbol'] = result['symbol'].str.decode('ascii')
            sf = self.key_suffix
            result['Time Stamp' + sf] = data['date'] + data['time']
            result.drop(['date', 'time'], axis=1, inplace=True)
            result.rename_axis({"symbol": "Symbol" + sf, "high_p": "High" + sf, "low_p": "Low" + sf, "open_p": "Open" + sf, "close_p": "Close" + sf, "tot_vlm": "Total Volume" + sf, "prd_vlm": "Period Volume" + sf, "num_trds": "Number of Trades" + sf}, axis="columns", copy=False, inplace=True)

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
