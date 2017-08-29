import pyevents.events as events
from atpy.data.iqfeed.iqfeed_latest_bars import IQFeedLatestBars
from atpy.ibapi.ib_events import IBEvents


class Environment(object, metaclass=events.GlobalRegister):
    def __init__(self, interval_len, interval_type='s', fire_bars=True, mkt_snapshot_depth=0, key_suffix=''):
        self.latest_bars = IQFeedLatestBars(interval_len=interval_len, interval_type=interval_type, fire_bars=fire_bars, mkt_snapshot_depth=mkt_snapshot_depth, key_suffix=key_suffix)
        self.ibapi = IBEvents("127.0.0.1", 4002, 0)

    def __enter__(self):
        self.latest_bars.__enter__()
        self.ibapi.__enter__()
        self.ibapi.reqPositions()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.latest_bars.__exit__(exception_type, exception_value, traceback)
        self.ibapi.__exit__(exception_type, exception_value, traceback)

    @events.listener
    def on_event(self, event):
        if event['type'] == 'ibapi_positions':
            self.latest_bars.watch_bars(list(event['data']['symbol'].unique()))
