from atpy.util.events_util import *


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
