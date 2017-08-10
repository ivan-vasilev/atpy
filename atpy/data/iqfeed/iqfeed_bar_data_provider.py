import pyevents.events as events
from atpy.data.iqfeed.util import *
from atpy.data.iqfeed.iqfeed_level_1_provider import Fundamentals
import pandas as pd
import threading


class IQFeedBarDataListener(iq.SilentBarListener, metaclass=events.GlobalRegister):

    def __init__(self, fire_bars=True, mkt_snapshot_depth=0, key_suffix=''):
        """
        :param fire_bars: raise event for each individual bar if True
        :param mkt_snapshot_depth: construct and maintain dataframe representing the current market snapshot with depth. If 0, then don't construct, otherwise construct for the past periods
        :param key_suffix: suffix to the fieldnames
        """
        super().__init__(name="Bar data listener")

        self.fire_bars = fire_bars
        self.key_suffix = key_suffix
        self.conn = None
        self.streaming_conn = None
        self.current_batch = None
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
            self.on_latest_bar_update(self._process_data(iqfeed_to_dict(np.copy(bar_data), key_suffix=self.key_suffix)))

        if self.mkt_snapshot_depth > 0:
            data = data if data is not None else self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
            self._update_mkt_snapshot(data)

    def process_live_bar(self, bar_data: np.array) -> None:
        data = None

        if self.fire_bars:
            data = self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
            self.on_bar(data)

        if self.mkt_snapshot_depth > 0:
            data = data if data is not None else self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
            self._update_mkt_snapshot(data)

    def process_history_bar(self, bar_data: np.array) -> None:
        bar_data = np.copy(bar_data)
        bd = bar_data[0] if len(bar_data) == 1 else bar_data
        adjust(bd, Fundamentals.get(bd['symbol'].decode('ascii'), self.streaming_conn))

        data = None
        if self.fire_bars:
            data = self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
            self.on_bar(data)

        if self.mkt_snapshot_depth > 0 is not None:
            data = data if data is not None else self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
            self._update_mkt_snapshot(data)

    @events.listener
    def on_event(self, event):
        if event['type'] == 'watch_bars':
            data = event['data']
            if isinstance(data['symbol'], list):
                data_copy = data.copy()
                for s in data_copy['symbol']:
                    if s not in self.watched_symbols:
                        data_copy['symbol'] = s
                        if self.mkt_snapshot_depth > 0:
                            data_copy['lookback_bars'] = self.mkt_snapshot_depth

                        self.conn.watch(**data_copy)
                        self.watched_symbols.add(s)
            elif data['symbol'] not in self.watched_symbols:
                self.conn.watch(**data)
                self.watched_symbols.add(data['symbol'])
        elif event['type'] == 'request_market_snapshot_bars':
            self.on_market_snapshot(self._mkt_snapshot)

    def _update_mkt_snapshot(self, data):
        if self.mkt_snapshot_depth is not None:
            with self._lock:
                if self._mkt_snapshot is None:
                    self._mkt_snapshot = pd.Series(data).to_frame().T
                    self._mkt_snapshot.set_index(['Symbol', 'Time Stamp'], append=False, inplace=True, drop=False)
                elif (data['Symbol'], data['Time Stamp']) in self._mkt_snapshot.index:
                    self._mkt_snapshot.loc[data['Symbol']].loc[data['Time Stamp']] = pd.Series(data)
                else:
                    mkts = self._mkt_snapshot

                    to_concat = pd.Series(data).to_frame().T
                    to_concat.set_index(['Symbol', 'Time Stamp'], append=False, inplace=True, drop=False)

                    mkts = pd.concat([mkts, to_concat])

                    multi_index = pd.MultiIndex.from_product([mkts.index.levels[0].unique(), mkts.index.levels[1].unique()], names=['Symbol', 'Time Stamp']).sort_values()

                    mkts = self._mkt_snapshot = mkts.reindex(multi_index)

                    for symbol in mkts.index.get_level_values('Symbol').unique():
                        mkts.loc[symbol, 'Time Stamp'] = mkts.index.levels[1]
                        mkts.loc[symbol, 'Symbol'] = symbol

                        for c in [c for c in ['Period Volume', 'Number of Trades'] if c in mkts.columns]:
                            mkts.loc[symbol, c].fillna(0, inplace=True)

                        if 'Open' in mkts.columns:
                            op = mkts.loc[symbol, 'Open']
                            op.fillna(method='ffill', inplace=True)

                            for c in [c for c in ['Close', 'High', 'Low'] if c in mkts.columns]:
                                mkts.loc[symbol, c].fillna(op, inplace=True)

                        mkts.loc[symbol].fillna(method='ffill', inplace=True)

                        mkts.loc[symbol].fillna(method='backfill', inplace=True)

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
    def on_market_snapshot(self, snapshot):
        return {'type': 'bar_market_snapshot', 'data': snapshot}
