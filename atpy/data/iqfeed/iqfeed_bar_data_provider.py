from dateutil import tz

import atpy.data.iqfeed.bar_util as bars
from atpy.data.iqfeed.iqfeed_level_1_provider import get_fundamentals
from atpy.data.iqfeed.util import *


class IQFeedBarDataListener(iq.SilentBarListener):

    def __init__(self, listeners, interval_len, interval_type='s', fire_bars=True, mkt_snapshot_depth=0, key_suffix=''):
        """
        :param fire_bars: raise event for each individual bar if True
        :param mkt_snapshot_depth: construct and maintain dataframe representing the current market snapshot with depth. If 0, then don't construct, otherwise construct for the past periods
        :param key_suffix: suffix to the fieldnames
        """
        super().__init__(name="Bar data listener")

        self.listeners = listeners
        self.listeners += self.on_event

        self.fire_bars = fire_bars
        self.key_suffix = key_suffix
        self.conn = None
        self.streaming_conn = None
        self.current_batch = None
        self.interval_len = interval_len
        self.interval_type = interval_type
        self.watched_symbols = set()
        self._lock = threading.RLock()

        self.mkt_snapshot_depth = mkt_snapshot_depth

        if mkt_snapshot_depth > 0:
            self._mkt_snapshot = None

        pd.set_option('mode.chained_assignment', 'warn')

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
        data = None

        if self.fire_bars:
            data = self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
            self.listeners({'type': 'latest_bar_update', 'data': data, 'interval_type': self.interval_type, 'interval_len': self.interval_len})

        if self.mkt_snapshot_depth > 0:
            data = data if data is not None else self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
            self._update_mkt_snapshot(data)

    def process_live_bar(self, bar_data: np.array) -> None:
        data = None

        if self.fire_bars:
            data = self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
            self.listeners({'type': 'bar', 'data': data, 'interval_type': self.interval_type, 'interval_len': self.interval_len})
        if self.mkt_snapshot_depth > 0:
            data = data if data is not None else self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
            self._update_mkt_snapshot(data)

    def process_history_bar(self, bar_data: np.array) -> None:
        bar_data = np.copy(bar_data)

        data = None
        if self.fire_bars:
            data = self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
            adjust(data, get_fundamentals(data['symbol'], self.streaming_conn))

            self.listeners({'type': 'bar', 'data': data, 'interval_type': self.interval_type, 'interval_len': self.interval_len})

        if self.mkt_snapshot_depth > 0 is not None:
            if data is None:
                data = self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
                adjust(data, get_fundamentals(data['symbol'], self.streaming_conn))

            self._update_mkt_snapshot(data)

    def on_event(self, event):
        if event['type'] == 'watch_bars':
            self.watch_bars(event['data']['symbol'] if isinstance(event['data'], dict) else event['data'])
        elif event['type'] == 'request_market_snapshot_bars':
            snapshot = self.request_market_snapshot_bars('normalize' in event and event['normalize'] is True)
            self.listeners({'type': 'bar_market_snapshot', 'data': snapshot, 'interval_type': self.interval_type, 'interval_len': self.interval_len})

    def watch_bars(self, symbol):
        if isinstance(symbol, str) and symbol not in self.watched_symbols:
            data_copy = {'symbol': symbol, 'interval_type': self.interval_type, 'interval_len': self.interval_len}
            if self.mkt_snapshot_depth > 0:
                data_copy['lookback_bars'] = self.mkt_snapshot_depth

            self.conn.watch(**data_copy)
            self.watched_symbols.add(symbol)
        elif isinstance(symbol, list):
            data_copy = dict(symbol=symbol)
            data_copy['interval_type'] = self.interval_type
            data_copy['interval_len'] = self.interval_len

            if self.mkt_snapshot_depth > 0:
                data_copy['lookback_bars'] = self.mkt_snapshot_depth

            for s in [s for s in data_copy['symbol'] if s not in self.watched_symbols]:
                data_copy['symbol'] = s
                self.conn.watch(**data_copy)
                self.watched_symbols.add(s)

        with self._lock:
            if self._mkt_snapshot is not None:
                symbol = list(self.watched_symbols)
                symbol.sort()

                multi_index = pd.MultiIndex.from_product([symbol, self._mkt_snapshot.index.levels[1].unique()], names=['symbol', 'timestamp']).sort_values()
                self._mkt_snapshot = bars.reindex_and_fill(self._mkt_snapshot, multi_index)

    def request_market_snapshot_bars(self, synchronize=False):
        with self._lock:
            snapshot = self._mkt_snapshot.dropna()
            snapshot.set_index(['symbol', 'timestamp'], inplace=True)
            snapshot.reset_index(inplace=True)
            snapshot.set_index(['symbol', 'timestamp'], drop=False, inplace=True)

            if synchronize:
                snapshot = bars.synchronize_timestamps(snapshot)

            return snapshot.groupby(level=0).tail(self.mkt_snapshot_depth)

    def _update_mkt_snapshot(self, data):
        if self.mkt_snapshot_depth is not None:
            with self._lock:
                if self._mkt_snapshot is None:
                    self._mkt_snapshot = pd.Series(data).to_frame().T
                    self._mkt_snapshot.set_index(['symbol', 'timestamp'], append=False, inplace=True, drop=False)

                    symbols = list(self.watched_symbols)
                    symbols.sort()

                    multi_index = pd.MultiIndex.from_product([symbols, self._mkt_snapshot.index.levels[1].unique()], names=['symbol', 'timestamp']).sort_values()
                    self._mkt_snapshot = bars.reindex_and_fill(self._mkt_snapshot, multi_index)
                elif isinstance(data, dict) and (data['symbol'], data['timestamp']) in self._mkt_snapshot.index:
                    self._mkt_snapshot.loc[data['symbol'], data['timestamp']] = pd.Series(data)
                else:
                    expand = data['timestamp'] > self._mkt_snapshot.index.levels[1][-1]

                    to_concat = pd.Series(data).to_frame().T
                    to_concat.set_index(['symbol', 'timestamp'], append=False, inplace=True, drop=False)

                    self._mkt_snapshot.update(to_concat)
                    to_concat = to_concat[~to_concat.index.isin(self._mkt_snapshot.index)]

                    self._mkt_snapshot = bars.merge_snapshots(self._mkt_snapshot, to_concat)

                    if expand:
                        self._mkt_snapshot = bars.expand(self._mkt_snapshot, min(1000, self.mkt_snapshot_depth * 10), self.mkt_snapshot_depth)

    def _process_data(self, data):
        if isinstance(data, dict):
            result = dict()

            sf = self.key_suffix

            result['symbol' + sf] = data.pop('symbol')
            result['timestamp' + sf] = (datetime.datetime.combine(data.pop('date'), datetime.datetime.min.time()) + data.pop('time').astype(datetime.datetime)).replace(tzinfo=tz.gettz('US/Eastern')).astimezone(tz.gettz('UTC'))
            result['high' + sf] = data.pop('high_p')
            result['low' + sf] = data.pop('low_p')
            result['open' + sf] = data.pop('open_p')
            result['close' + sf] = data.pop('close_p')
            result['total_volume' + sf] = data.pop('tot_vlm')
            result['period_volume' + sf] = data.pop('prd_vlm')
            result['number_of_trades' + sf] = data.pop('num_trds')
        else:
            result = pd.DataFrame(data)
            result['symbol'] = result['symbol'].str.decode('ascii')
            sf = self.key_suffix

            result['timestamp' + sf] = pd.Index(data['date'] + data['time'].astype(datetime.datetime)).tz_localize('US/Eastern').tz_convert('UTC')

            result.set_index('timestamp' + sf, inplace=True, drop=False)

            result.drop(['date', 'time'], axis=1, inplace=True)
            result.rename({"symbol": "symbol" + sf, "high_p": "high" + sf, "low_p": "low" + sf, "open_p": "open" + sf, "close_p": "close" + sf, "tot_vlm": "total_volume" + sf, "prd_vlm": "period_volume" + sf, "num_trds": "number_of_trades" + sf}, axis="columns", copy=False, inplace=True)

        return result

    def bar_provider(self):
        return IQFeedDataProvider(self.listeners,  accept_event=lambda e: True if e['type'] == 'bar' else False)
