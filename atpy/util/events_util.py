from atpy.util.singleton import *
from pyevents.events import *


class GlobalListeners(ChainedLists, metaclass=SingletonType):
    """Singleton ilst for the purpose of holding global listeners"""
