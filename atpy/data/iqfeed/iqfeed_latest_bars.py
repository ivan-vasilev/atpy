import pyevents.events as events


class IQFeedLatestBars(metaclass=events.GlobalRegister):
    def __init__(self, interval_type: str, interval_len: int, max_ticks: int, historical_symbols: list = None, streaming_symbols: list = None):
        self.interval_type = interval_type
        self.interval_len = interval_len
        self.max_ticks = max_ticks
        self.historical_symbols = historical_symbols
        self.streaming_symbols = streaming_symbols
