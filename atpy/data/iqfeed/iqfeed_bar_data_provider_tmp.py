import typing

from dateutil import tz

from atpy.data.iqfeed.iqfeed_level_1_provider import get_splits_dividends
from atpy.data.iqfeed.util import *
from atpy.data.splits_dividends import adjust_df
from pyevents.events import EventFilter


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
        self.watched_symbols = dict()

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
            del self.watched_symbols[bad_symbol]

    def process_latest_bar_update(self, bar_data: np.array) -> None:
        bar_data = bar_data[0] if len(bar_data) == 1 else bar_data

        symbol = bar_data[0].decode("utf-8")

        data = self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
        self.listeners({'type': 'latest_bar_update', 'data': data, 'interval_type': self.interval_type, 'interval_len': self.interval_len})

    def latest_bar_event_stream(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if 'type' in e and e['type'] == 'latest_bar_update' else False,
                           event_transformer=lambda e: e['data'])

    def process_live_bar(self, bar_data: np.array) -> None:
        data = self._process_data(iqfeed_to_dict(bar_data, key_suffix=self.key_suffix))
        self.listeners({'type': 'bar', 'data': data, 'interval_type': self.interval_type, 'interval_len': self.interval_len})

    def process_history_bar(self, bar_data: np.array) -> None:
        bar_data = (bar_data[0] if len(bar_data) == 1 else bar_data).copy()

        symbol = bar_data[0].decode("utf-8")

        self.watched_symbols[symbol].append(bar_data)

        if len(self.watched_symbols[symbol]) == self.mkt_snapshot_depth:
            df = pd.DataFrame(create_batch(self.watched_symbols[symbol]))

            df['timestamp'] = pd.Index(df['date'] + df['time']).tz_localize('US/Eastern').tz_convert('UTC')

            df.drop(['date', 'time'], inplace=True, axis=1)

            df.rename(index=str,
                      columns={'open_p': 'open',
                               'high_p': 'high',
                               'low_p': 'low',
                               'close_p': 'close',
                               'tot_vlm': 'total_volume',
                               'prd_vlm': 'period_volume',
                               'num_trds': 'number_of_trades'},
                      inplace=True)

            df.set_index(['symbol', 'timestamp'], drop=True, inplace=True, append=False)

            adjust_df(df, get_splits_dividends(symbol, self.streaming_conn))

            self.watched_symbols[symbol] = df

            self.listeners({'type': 'bar', 'data': df, 'interval_type': self.interval_type, 'interval_len': self.interval_len})

    def bar_event_stream(self):
        return EventFilter(listeners=self.listeners,
                           event_filter=lambda e: True if 'type' in e and e['type'] == 'bar' else False,
                           event_transformer=lambda e: e['data'])

    def on_event(self, event):
        if event['type'] == 'watch_bars':
            self.watch_bars(event['data']['symbol'] if isinstance(event['data'], dict) else event['data'])

    def watch_bars(self, symbol: typing.Union[str, list]):
        if isinstance(symbol, str) and symbol not in self.watched_symbols:
            data_copy = {'symbol': symbol, 'interval_type': self.interval_type, 'interval_len': self.interval_len}
            if self.mkt_snapshot_depth > 0:
                data_copy['lookback_bars'] = self.mkt_snapshot_depth

            self.conn.watch(**data_copy)
            self.watched_symbols[symbol] = list()
        elif isinstance(symbol, list):
            data_copy = dict(symbol=symbol)
            data_copy['interval_type'] = self.interval_type
            data_copy['interval_len'] = self.interval_len

            if self.mkt_snapshot_depth > 0:
                data_copy['lookback_bars'] = self.mkt_snapshot_depth

            for s in [s for s in data_copy['symbol'] if s not in self.watched_symbols]:
                data_copy['symbol'] = s
                self.conn.watch(**data_copy)
                self.watched_symbols[s] = list()

    def _bar_to_df(self, data):
        result = dict()

        sf = self.key_suffix

        result['symbol' + sf] = data.pop('symbol')

        result['timestamp' + sf] = (datetime.datetime.combine(data.pop('date'), datetime.datetime.min.time())
                                    + datetime.timedelta(microseconds=data.pop('time').astype(np.uint64) / 1)) \
            .replace(tzinfo=tz.gettz('US/Eastern')).astimezone(tz.gettz('UTC'))

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
