class BaseEvent(object):
    """Base event class with a single callback property"""

    def __init__(self, data=None, callback=None):
        """
        :param data: data accompanying the event
        :param callback: callback to be called after the event is consumed
        """
        self.data = data
        self.callback = callback

    def __getattr__(self, name):
        if self.data is not None:
            return getattr(self.data, name)
        else:
            raise AttributeError

    def __getitem__(self, key):
        if self.data is not None and hasattr(self.data, "__getitem__"):
            return self.data.__getitem__(key)
        else:
            raise NotImplementedError


class BarEvent(BaseEvent):
    pass


class BarBatchEvent(BaseEvent):
    pass


class HistoryEvent(BaseEvent):
    pass


class HistoryBatchEvent(BaseEvent):
    pass


class NewsMinibatchEvent(BaseEvent):
    pass


class NewsBatchEvent(BaseEvent):
    pass


class Level1NewsItemEvent(BaseEvent):
    pass


class Level1NewsBatchEvent(BaseEvent):
    pass


class Level1RegionalQuoteEvent(BaseEvent):
    pass


class Level1RegionalQuoteBatchEvent(BaseEvent):
    pass


class Level1SummaryEvent(BaseEvent):
    pass


class Level1SummaryBatchEvent(BaseEvent):
    pass


class Level1UpdateEvent(BaseEvent):
    pass


class Level1UpdateBatchEvent(BaseEvent):
    pass


class Level1FundamentalsEvent(BaseEvent):
    pass


class Level1FundamentalsBatchEvent(BaseEvent):
    pass
