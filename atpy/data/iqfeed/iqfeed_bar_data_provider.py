from dateutil import tz

from atpy.data.iqfeed.iqfeed_level_1_provider import get_fundamentals
from atpy.data.iqfeed.util import *


class IQFeedBarDataListener(iq.SilentBarListener):

    def __init__(self, listeners, interval_len, interval_type='s', mkt_snapshot_depth=0, key_suffix=''):
        """
        :param mkt_snapshot_depth: construct and maintain dataframe representing the current market snapshot with depth. If 0, then don't construct, otherwise construct for the past periods
        :param key_suffix: suffix to the fieldnames
        """
        super().__init__(name="Bar data listener")

        self.listeners = listeners
        self.listeners += self.on_event

        self.key_suffix = key_suffix
        self.conn = None
        self.streaming_conn = None
        self.interval_len = interval_len
        self.interval_type = interval_type
        self.mkt_snapshot_depth = mkt_snapshot_depth
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

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        if bad_symbol in self.watched_symbols and bad_symbol in self.watched_symbols:
            self.watched_symbols.remove(bad_symbol)

    def process_latest_bar_update(self, bar_data: np.array) -> None:
        data = self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
        self.listeners({'type': 'latest_bar_update', 'data': data, 'interval_type': self.interval_type, 'interval_len': self.interval_len})

    def process_live_bar(self, bar_data: np.array) -> None:
        data = self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
        self.listeners({'type': 'bar', 'data': data, 'interval_type': self.interval_type, 'interval_len': self.interval_len})

    def process_history_bar(self, bar_data: np.array) -> None:
        data = self._process_data(iqfeed_to_dict(np.copy(bar_data), key_suffix=self.key_suffix))

        adjust(data, get_fundamentals(data['symbol'], self.streaming_conn))

        self.listeners({'type': 'bar', 'data': data, 'interval_type': self.interval_type, 'interval_len': self.interval_len})

    def on_event(self, event):
        if event['type'] == 'watch_bars':
            self.watch_bars(event['data']['symbol'] if isinstance(event['data'], dict) else event['data'])

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

    def _process_data(self, data):
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

        result = pd.DataFrame(result, index=pd.MultiIndex.from_tuples([(result['timestamp'], result['symbol'])], names=['timestamp', 'symbol']))

        return result

    def bar_provider(self):
        return IQFeedDataProvider(self.listeners, accept_event=lambda e: True if e['type'] == 'bar' else False)
