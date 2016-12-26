from abc import *


class DataProvider(metaclass=ABCMeta):

    @abstractmethod
    def __iter__(self):
        return

    @abstractmethod
    def __next__(self) -> map:
        return
