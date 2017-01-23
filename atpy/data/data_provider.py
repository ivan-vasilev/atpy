from abc import *
import threading
import queue
from typing import Generator


class DataProvider(metaclass=ABCMeta):
    """Base data provider generator/iterator interface"""

    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __next__(self) -> dict:
        pass


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

        self.is_running = False
        self.producer_thread = None
        self.generator = generator

    def start(self):
        def fill_queue():
            for d in self.generator():
                self.put(d)
                if not self.is_running:
                    break

        self.producer_thread = threading.Thread(target=fill_queue, daemon=True)
        self.is_running = True
        self.producer_thread.start()

    def stop(self):
        self.is_running = False
