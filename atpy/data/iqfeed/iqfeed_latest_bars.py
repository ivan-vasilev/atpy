import pyevents.events as events
from atpy.data.iqfeed.iqfeed_bar_data_provider import IQFeedBarDataListener
import threading
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider, BarsFilter


class IQFeedLatestBars(IQFeedBarDataListener):
    def __enter__(self):
        super().__enter__()

        self.history = IQFeedHistoryProvider(num_connections=2, key_suffix=self.key_suffix, lmdb_path=None)
        self.history.__enter__()

        self.symbols = list()

        self._is_running = True

        def generate_data():
            while self._is_running:
                self._merge_history()

        threading.Thread(target=generate_data, daemon=True).start()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        super().__exit__(exception_type, exception_value, traceback)

        self._is_running = False

        self.history.__exit__(exception_type, exception_value, traceback)

    def _merge_history(self):
        with self._lock:
            if len(self.symbols) == 0:
                return

        result = self.history.request_data(BarsFilter(self.symbols, self.interval_len, self.interval_type, self.mkt_snapshot_depth), synchronize_timestamps=True)
        with self._lock:
            if self._mkt_snapshot is not None:
                if not result.index.isin(self._mkt_snapshot.index).all():
                    ind = result[~result.index.isin(self._mkt_snapshot.index)]
                    self._mkt_snapshot = self._merge_snapshots(self._mkt_snapshot, ind)
            else:
                self._mkt_snapshot = result

    @events.listener
    def on_event(self, event):
        super().on_event(event)

        if event['type'] == 'watch_history_bars':
            data = event['data']

            with self._lock:
                if isinstance(data, list):
                    for s in [s for s in data if s not in self.symbols]:
                        self.symbols.append(s)
                elif isinstance(data, str) and data not in self.symbols:
                    self.symbols.append(data)
