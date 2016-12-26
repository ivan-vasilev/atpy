from abc import *


class base_event(metaclass=ABCMeta):
    """
    Base abstract event annotation.
    """

    def __init__(self, func):
        self.__doc__ = func.__doc__
        self._key = ' ' + func.__name__
        self._fns = list()
        self._func = func

    def __iadd__(self, fn):
        self._fns.append(fn)
        return self

    def __isub__(self, fn):
        self._fns.remove(fn)
        return self

    @abstractmethod
    def __call__(self, *args, **kwargs):
        for f in self._fns:
            f(*args, **kwargs)


class before(base_event):
    """
    Notifies listeners before method execution. Use like
    >>> @before
    >>> def method_with_before():
    >>>     print("method_with_before called")
    >>> method_with_before += lambda: print("Listeners before")
    >>> method_with_before()
    """
    def __init__(self, func):
        super().__init__(func)

    def __call__(self, *args, **kwargs):
        super.__call__(*args, **kwargs)
        self._func(*args, **kwargs)


class after(base_event):
    """
    Notifies listeners after method execution. Use like
    >>> @after
    >>> def method_with_after():
    >>>     print("method_with_after called")
    >>> method_with_after += lambda: print("Listeners after")
    >>> method_with_after()
    """
    def __init__(self, func):
        super().__init__(func)

    def __call__(self, *args, **kwargs):
        self._func(*args, **kwargs)
        super().__call__(*args, **kwargs)


class event(base_event):
    """
    Notifies 2 sets of listeners - before and after. Use like
    >>> @event
    >>> def method_with_event():
    >>>     print("method_with_event called")
    >>> method_with_event.before += lambda: print("Listeners before")
    >>> method_with_event.after += lambda: print("Listeners after")
    >>> method_with_event()
    """

    def __init__(self, func):
        super().__init__(func)

        class __base_event(base_event):
            def __init__(self, func):
                super().__init__(func)

            def __call__(self, *args, **kwargs):
                super().__call__(*args, **kwargs)

        self.before = __base_event(func)
        self.after = __base_event(func)

    def __call__(self, *args, **kwargs):
        self.before.__call__(*args, **kwargs)
        self._func(*args, **kwargs)
        self.after.__call__(*args, **kwargs)
