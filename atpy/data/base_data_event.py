class BaseDataEvent(object):
    """Base event class with a single callback property"""

    def __init__(self, data=None, phase=None):
        """
        :param data: data accompanying the event
        :param phase: data phase
        """
        self.phase = phase
        self.data = data

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
