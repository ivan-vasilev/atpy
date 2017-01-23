from atpy.util.singleton import *
from pyevents.events import *


class EventBuss(object, metaclass=SingletonType):
    """Singleton, used for global event bus"""

    @before
    def fire(self, *args, **kwargs):
        pass
