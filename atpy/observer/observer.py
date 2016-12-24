class event(object):
    def __init__(self, func):
        self.__doc__ = func.__doc__
        self._key = ' ' + func.__name__
        self._fns = []
        self._func = func

    def __iadd__(self, fn):
        self._fns.append(fn)
        return self

    def __isub__(self, fn):
        self._fns.remove(fn)
        return self

    def __call__(self, *args, **kwargs):
        for f in self._fns[:]:
            f(*args, **kwargs)

        self._func(*args, **kwargs)
