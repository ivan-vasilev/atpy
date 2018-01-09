from atpy.data.iqfeed.iqfeed_bar_data_provider import IQFeedBarDataListener
from atpy.data.iqfeed.iqfeed_level_1_provider import IQFeedLevel1Listener
from atpy.ibapi.ib_events import IBEvents


class Environment(object):
    def __init__(self, listeners, interval_len, interval_type='s', fire_bars=True, fire_news=True, mkt_snapshot_depth=0, key_suffix=''):
        self.listeners = listeners
        self.listeners += self.on_event

        self.latest_bars = IQFeedBarDataListener(listeners=listeners, interval_len=interval_len, interval_type=interval_type, fire_bars=fire_bars, mkt_snapshot_depth=mkt_snapshot_depth, key_suffix=key_suffix)
        self.ibapi = IBEvents(listeners=listeners, ipaddress="127.0.0.1", portid=4002,  clientid=0)
        self.fire_news = fire_news
        self.level_1_conn = IQFeedLevel1Listener(listeners=listeners, fire_ticks=False) if fire_news else None

    def __enter__(self):
        self.latest_bars.__enter__()
        self.ibapi.__enter__()
        self.ibapi.reqPositions()

        if self.level_1_conn is not None:
            self.level_1_conn.__enter__()
            self.level_1_conn.news_on()

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.latest_bars.__exit__(exception_type, exception_value, traceback)
        self.ibapi.__exit__(exception_type, exception_value, traceback)

        if self.level_1_conn is not None:
            self.level_1_conn.__exit__(exception_type, exception_value, traceback)

    def on_event(self, event):
        if event['type'] == 'ibapi_positions':
            self.latest_bars.watch_bars(list(event['data']['symbol'].unique()))
