import pyevents.events as events
from atpy.data.iqfeed.util import *
from atpy.data.iqfeed.iqfeed_level_1_provider import Fundamentals
import pandas as pd
import threading


class IQFeedBarDataListener(iq.SilentBarListener, metaclass=events.GlobalRegister):

    def __init__(self, fire_bars=True, minibatch=None, mkt_snapshot_minibatch=None, mkt_snapshot=False, key_suffix=''):
        """
        :param fire_bars: raise event for each individual bar if True
        :param minibatch: size of the minibatch
        :param mkt_snapshot_minibatch: fire events, containing latest prices 
        :param key_suffix: suffix to the fieldnames
        """
        super().__init__(name="Bar data listener")

        self.fire_bars = fire_bars
        self.minibatch = minibatch
        self._current_mkt_snapshot = dict()
        self.key_suffix = key_suffix
        self.conn = None
        self.streaming_conn = None
        self.current_batch = None
        self.watched_symbols = set()
        self._lock = threading.RLock()

        self.mkt_snapshot_minibatch = mkt_snapshot_minibatch
        self.mkt_snapshot = mkt_snapshot

        if mkt_snapshot_minibatch is not None:
            self._unique_bars = set()
            self._mb_mkt_snapshot = dict()

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

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        if bad_symbol in self.watched_symbols and bad_symbol in self.watched_symbols:
            self.watched_symbols.remove(bad_symbol)

    def process_latest_bar_update(self, bar_data: np.array) -> None:
        bar_data = np.copy(bar_data)

        bd = bar_data[0] if len(bar_data) == 1 else bar_data

        self._current_mkt_snapshot[bd['symbol']] = bd

        if self.fire_bars:
            self.on_latest_bar_update(self._process_data(iqfeed_to_dict(np.copy(bar_data), key_suffix=self.key_suffix)))

    def process_live_bar(self, bar_data: np.array) -> None:
        bd = np.copy(bar_data)[0]

        self._on_bar(bd)

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
            data = event['data']
            if isinstance(data['symbol'], list):
                data_copy = data.copy()
                for s in data_copy['symbol']:
                    if s not in self.watched_symbols:
                        data_copy['symbol'] = s
                        self.conn.watch(**data_copy)
                        self.watched_symbols.add(s)
            elif data['symbol'] not in self.watched_symbols:
                self.conn.watch(**data)
                self.watched_symbols.add(data['symbol'])
        elif event['type'] == 'request_market_snapshot_bars':
            self.on_market_snapshot(self._market_snapshot)

    @property
    def _market_snapshot(self):
        with self._lock:
            arr = np.empty((len(self._current_mkt_snapshot),), iq.BarConn.interval_data_type)
            for i, v in enumerate(self._current_mkt_snapshot.values()):
                arr[i] = v

        bar_dataframe = self._process_data(arr)
        bar_dataframe.sort_values('Time Stamp')

        return bar_dataframe

    def _on_bar(self, bar_data):
        self._current_mkt_snapshot[bar_data['symbol']] = bar_data

        if self.minibatch is not None:
            with self._lock:
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

        if self.mkt_snapshot_minibatch is not None:
            with self._lock:
                self._unique_bars.add(bar_data['date'] + bar_data['time'])
                symbol = bar_data['symbol'].decode('ascii')
                if symbol not in self._mb_mkt_snapshot:
                    self._mb_mkt_snapshot[symbol] = list()

                self._mb_mkt_snapshot[symbol].append(bar_data)

                if len(self._unique_bars) == self.mkt_snapshot_minibatch:
                    ts = pd.Series(data=list(self._unique_bars), name='Time Stamp' + self.key_suffix)
                    ts.sort_values(inplace=True)
                    ts = ts.to_frame()

                    for symbol, signal in self._mb_mkt_snapshot.items():
                        df = self._process_data(np.array(self._mb_mkt_snapshot[symbol], dtype=iq.BarConn.interval_data_type))
                        df = pd.merge_ordered(df, ts, on='Time Stamp', how='outer', fill_method='ffill')
                        df.fillna(method='backfill', inplace=True)

                        self._mb_mkt_snapshot[symbol] = df

                    batch = pd.Panel.from_dict(self._mb_mkt_snapshot)
                    self.on_mb_market_snapshot(batch)

                    self._mb_mkt_snapshot = dict()
                    self._unique_bars = set()

        if self.mkt_snapshot is not None:
            with self._lock:
                self._unique_bars.add(bar_data['date'] + bar_data['time'])
                symbol = bar_data['symbol'].decode('ascii')
                if symbol not in self._mb_mkt_snapshot:
                    self._mb_mkt_snapshot[symbol] = list()

                self._mb_mkt_snapshot[symbol].append(bar_data)

                if len(self._unique_bars) == self.mkt_snapshot_minibatch:
                    ts = pd.Series(data=list(self._unique_bars), name='Time Stamp' + self.key_suffix)
                    ts.sort_values(inplace=True)
                    ts = ts.to_frame()

                    for symbol, signal in self._mb_mkt_snapshot.items():
                        df = self._process_data(np.array(self._mb_mkt_snapshot[symbol], dtype=iq.BarConn.interval_data_type))
                        df = pd.merge_ordered(df, ts, on='Time Stamp', how='outer', fill_method='ffill')
                        df.fillna(method='backfill', inplace=True)

                        self._mb_mkt_snapshot[symbol] = df

                    batch = pd.Panel.from_dict(self._mb_mkt_snapshot)
                    self.on_mb_market_snapshot(batch)

                    self._mb_mkt_snapshot = dict()
                    self._unique_bars = set()

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

            result.set_index('Time Stamp' + sf, inplace=True, drop=False)

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

    @events.after
    def on_market_snapshot(self, snapshot):
        return {'type': 'bar_market_snapshot', 'data': snapshot}

    @events.after
    def on_mb_market_snapshot(self, snapshot):
        return {'type': 'mb_bar_market_snapshot', 'data': snapshot}
