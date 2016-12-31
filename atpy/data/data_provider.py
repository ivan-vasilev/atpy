from abc import *
import threading
import queue
from typing import Generator


class DataProvider(metaclass=ABCMeta):
    """Base data provider generator/iterator interface"""

    @abstractmethod
    def __iter__(self):
        return

    @abstractmethod
    def __next__(self) -> map:
        return


class PCQueue(queue.Queue):

    """Asynchronous helper for data generation tasks. Use like:
    >>> def produce():
    >>>     for i in range(100):
    >>>         yield i
    >>>
    >>> test = PCQueue(produce)
    >>> test.start()
    >>>
    >>> while not test.empty():
    >>>     print(test.get())
    """

    def __init__(self, generator: Generator, maxsize=0):
        super().__init__(maxsize=maxsize)

        def fill_queue():
            for d in generator():
                self.put(d)

        self.producer_thread = threading.Thread(target=fill_queue, daemon=True)

    def start(self):
        self.producer_thread.start()
