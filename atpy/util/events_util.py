from atpy.util.singleton import *
from pyevents.events import *


class GlobalListeners(ChainedLists, metaclass=SingletonType):
    """Singleton ilst for the purpose of holding global listeners"""


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

