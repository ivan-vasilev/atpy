import datetime
import logging
import queue
import threading
import typing

import numpy as np
import pandas as pd
from pandas.util.testing import assert_frame_equal


class DataReplay(object):
    """Replay data from multiple sources, sorted by time"""

    def __init__(self):
        self._sources = list()
        self._is_running = False

    def __enter__(self):
        self._is_running = True

        self._threads = list()

        self._data = dict()

        self._timeline = None

        sources = dict()

        for (iterator, name, run_async) in self._sources:
            if run_async:
                q = queue.Queue()
                sources[name] = q

                t = _DataGeneratorThread(q=q, next_item=iterator)
                self._threads.append(t)
                t.start()
            else:
                sources[name] = iterator

        self._sources = sources

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        for t in self._threads:
            t.stop()

        self._is_running = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._timeline is not None:
            for e in list(self._data.keys()):
                _, ind = self._get_datetime_level(self._data[e].index)

                if ind[-1] < self._timeline.index[self._current_time]:
                    del self._data[e]

        for e in self._sources.keys() - self._data.keys():
            now = datetime.datetime.now()

            dp = self._sources[e]
            try:
                df = dp.get() if isinstance(dp, queue.Queue) else next(dp)
                level, ind = self._get_datetime_level(df)
                if level != 0:
                    df = df.swaplevel(0, level)
                    df.sort_values(ind.name, axis=0, inplace=True)
            except StopIteration:
                df = None

            if df is not None:
                self._data[e] = df
                logging.getLogger(__name__).debug('Obtained data ' + str(e) + ' in ' + str(datetime.datetime.now() - now))
            else:
                del self._sources[e]

        if self._timeline is None and len(self._data) > 0:
            now = datetime.datetime.now()

            indices = [self._get_datetime_level(df.index)[1] for df in self._data.values()]
            tzs = {ind.tz for ind in indices}

            if len(tzs) > 1:
                raise Exception("Multiple timezones detected")

            ind = pd.DatetimeIndex(np.hstack(indices)).unique().tz_localize(next(iter(tzs))).sort_values()

            self._timeline = pd.DataFrame(index=ind)
            self._current_time = 0

            for e, df in self._data.items():
                ind = self._get_datetime_level(df.index)[1]
                self._timeline[e] = False
                self._timeline.loc[ind, e] = True

            logging.getLogger(__name__).debug('Built timeline in ' + str(datetime.datetime.now() - now))

        if self._timeline is not None:
            result = dict()
            row = self._timeline.iloc[self._current_time]
            for e in [e for e in row.index if row[e]]:
                df = self._data[e]
                l, ind = self._get_datetime_level(df)
                result[e] = df.xs(self._timeline.index[self._current_time], level=l if isinstance(df.index, pd.MultiIndex) else None, drop_level=True)

            self._current_time += 1

            if self._current_time == len(self._timeline):
                self._timeline = None
                self._current_time = None
                self._data = dict()

            return result
        else:
            raise StopIteration()

    @staticmethod
    def _get_datetime_level(index):
        if isinstance(index, pd.DataFrame) or isinstance(index, pd.Series):
            index = index.index

        if isinstance(index, pd.DatetimeIndex):
            return 0, index
        elif isinstance(index, pd.MultiIndex):
            for i, l in enumerate(index.levels):
                if isinstance(l, pd.DatetimeIndex):
                    return i, l

    def add_source(self, data_provider: typing.Union[typing.Iterator, typing.Callable], name: str, run_async: bool = False):
        """
        :param data_provider: return pd.DataFrame with either DateTimeIndex or MultiIndex, where one of the levels is of datetime type
        :param name: data set name for each of the data sources
        :param run_async: whether to retrieve data synchronously or asynchronously
        :return:
        """
        if self._is_running:
            raise Exception("Cannot add sources while the generator is working")

        self._sources.append((data_provider, name, run_async))

        return self


class _DataGeneratorThread(threading.Thread):

    def __init__(self, q: queue.Queue, next_item: typing.Union[typing.Iterator, typing.Callable]):
        super().__init__(target=self.run, daemon=True)
        self.q = q
        self.next_item = next_item
        self._is_running = False

    def run(self):
        self._is_running = True

        while self._is_running:
            try:
                self.q.put(next(self.next_item))
            except StopIteration:
                self.q.put(None)
                self.stop()
            except Exception:
                item = self.next_item()
                self.q.put(item)
                if item is None:
                    self.stop()

    def stop(self):
        self._is_running = False


class DataReplayEvents(object):

    def __init__(self, listeners, data_replay: DataReplay):
        self.listeners = listeners
        self.data_replay = data_replay
        self.listeners += self.on_event

    def start(self):
        self.listeners(next(self.data_replay))

    def on_event(self, event):
        if event['type'] == 'cycle_finished':
            self.listeners(next(self.data_replay))
