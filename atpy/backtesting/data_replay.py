import datetime
import logging
import queue
import threading
import typing

import numpy as np
import pandas as pd


class DataReplay(object):
    """Replay data from multiple sources, sorted by time. Each source provides a dataframe."""

    def __init__(self):
        self._sources = list()
        self._is_running = False

    def __enter__(self):
        self._is_running = True

        self._threads = list()

        self._data = dict()

        self._timeline = None

        sources = dict()

        for (iterator, name, run_async, historical_depth) in self._sources:
            if run_async:
                q = queue.Queue()
                sources[name] = (q, historical_depth)

                self._threads.append(_DataGeneratorThread(q=q, next_item=iterator))
            else:
                sources[name] = (iterator, historical_depth)

        self._sources = sources

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        for t in self._threads:
            t.stop()

        self._is_running = False

    def __iter__(self):
        for t in self._threads:
            t.start()

        return self

    def __next__(self):
        # delete "expired" dataframes
        old_data = None
        if self._timeline is not None:
            # check for timeline end reset if necessary
            if self._current_time == len(self._timeline):
                self._timeline = None
                self._current_time = None
                old_data = self._data
                self._data = dict()
            else:
                for e in list(self._data.keys()):
                    if self._get_datetime_level(self._data[e].index)[-1] < self._timeline.index[self._current_time]:
                        if old_data is None:
                            old_data = dict()

                        old_data[e] = self._data[e]
                        del self._data[e]

        # request new dataframes if needed
        for e in self._sources.keys() - self._data.keys():
            now = datetime.datetime.now()

            dp, _ = self._sources[e]
            try:
                df = dp.get() if isinstance(dp, queue.Queue) else next(dp)
            except StopIteration:
                df = None

            if df is not None:
                self._data[e] = df
                logging.getLogger(__name__).debug('Obtained data ' + str(e) + ' in ' + str(datetime.datetime.now() - now))
            else:
                del self._sources[e]
                if old_data and e in old_data:
                    del old_data[e]

        # build timeline
        if self._timeline is None and self._data:
            now = datetime.datetime.now()

            indices = [self._get_datetime_level(df.index) for df in self._data.values()]
            tzs = {ind.tz for ind in indices}

            if len(tzs) > 1:
                raise Exception("Multiple timezones detected")

            ind = pd.DatetimeIndex(np.hstack(indices)).unique().tz_localize(next(iter(tzs))).sort_values()

            self._timeline = pd.DataFrame(index=ind)
            self._current_time = 0

            for e, df in self._data.items():
                ind = self._get_datetime_level(df.index)
                self._timeline[e] = False
                self._timeline.loc[ind, e] = True

            logging.getLogger(__name__).debug('Built timeline in ' + str(datetime.datetime.now() - now))

        # prepend old data for continuity
        if old_data:
            for e, old_df in old_data.items():
                _, historical_depth = self._sources[e]
                if historical_depth > 0:
                    ind = self._get_datetime_level(old_df)
                    old_df_slice = old_df.loc[slice(ind[max(-len(ind), -historical_depth)], ind[-1]), :]
                    self._data[e] = pd.concat((old_df_slice, self._data[e]))

        # produce results
        if self._timeline is not None:
            result = dict()

            current_time = self._timeline.index[self._current_time]

            row = self._timeline.iloc[self._current_time]

            for e in [e for e in row.index if row[e]]:
                df = self._data[e]
                _, historical_depth = self._sources[e]
                ind = self._get_datetime_level(df)
                result[e] = df.loc[ind[max(0, ind.get_loc(current_time) - historical_depth)]:current_time]

            self._current_time += 1

            return result
        else:
            raise StopIteration()

    @staticmethod
    def _get_datetime_level(index):
        if isinstance(index, pd.DataFrame) or isinstance(index, pd.Series):
            index = index.index

        if isinstance(index, pd.DatetimeIndex):
            return index
        elif isinstance(index, pd.MultiIndex):
            return [l for l in index.levels if isinstance(l, pd.DatetimeIndex)][0]

    def add_source(self, data_provider: typing.Union[typing.Iterator, typing.Callable], name: str, run_async: bool = False, historical_depth: int = 0):
        """
        :param data_provider: return pd.DataFrame with either DateTimeIndex or MultiIndex, where one of the levels is of datetime type
        :param name: data set name for each of the data sources
        :param run_async: whether to retrieve data synchronously or asynchronously
        :param historical_depth: whether to return only the current element or with historical depth
        :return:
        """
        if self._is_running:
            raise Exception("Cannot add sources while the generator is working")

        self._sources.append((data_provider, name, run_async, historical_depth))

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

    def __init__(self, listeners, data_replay: DataReplay, event_name: str):
        self.listeners = listeners
        self.data_replay = data_replay
        self.event_name = event_name

    def start(self):
        for d in self.data_replay:
            self.listeners({'type': self.event_name, 'data': d})
