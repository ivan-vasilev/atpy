import datetime
import logging
import queue
import typing

import numpy as np
import pandas as pd


class DataReplay(object):
    """Replay data from multiple sources, sorted by time. Each source provides a dataframe."""

    def __init__(self):
        self._sources_defs = list()
        self._is_running = False

    def __iter__(self):
        if self._is_running:
            raise Exception("Cannot start iteration while the generator is working")

        self._is_running = True

        self._data = dict()

        self._timeline = None

        self._current_time = None

        sources = dict()

        for (iterator, name, historical_depth) in self._sources_defs:
            sources[name] = (iter(iterator), historical_depth)

        self._sources = sources

        return self

    def __next__(self):
        # delete "expired" dataframes
        old_data = None
        if self._timeline is not None and self._current_time is not None:
            # check for timeline end reset if necessary
            for e in list(self._data.keys()):
                if self._get_datetime_level(self._data[e].index)[-1] <= self._current_time:
                    if old_data is None:
                        old_data = dict()

                    old_data[e] = self._data[e]
                    del self._data[e]
                    self._timeline = None

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

            del old_data

        # produce results
        if self._timeline is not None:
            result = dict()

            if self._current_time is None or self._current_time < self._timeline.index[0]:
                self._current_time, current_index = self._timeline.index[0], 0
            elif self._current_time in self._timeline.index:
                current_index = self._timeline.index.get_loc(self._current_time) + 1
                self._current_time = self._timeline.index[current_index]
            else:
                self._current_time = self._timeline.loc[self._timeline.index > self._current_time].iloc[0].name
                current_index = self._timeline.index.get_loc(self._current_time)

            row = self._timeline.iloc[current_index]

            for e in [e for e in row.index if row[e]]:
                df = self._data[e]
                _, historical_depth = self._sources[e]
                ind = self._get_datetime_level(df)
                result[e] = df.loc[ind[max(0, ind.get_loc(self._current_time) - historical_depth)]:self._current_time]

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

    def add_source(self, data_provider: typing.Union[typing.Iterator, typing.Callable], name: str, historical_depth: int = 0):
        """
        :param data_provider: return pd.DataFrame with either DateTimeIndex or MultiIndex, where one of the levels is of datetime type
        :param name: data set name for each of the data sources
        :param historical_depth: whether to return only the current element or with historical depth
        :return:
        """
        if self._is_running:
            raise Exception("Cannot add sources while the generator is working")

        self._sources_defs.append((data_provider, name, historical_depth))

        return self


class DataReplayEvents(object):

    def __init__(self, listeners, data_replay: DataReplay, event_name: str):
        self.listeners = listeners
        self.data_replay = data_replay
        self.event_name = event_name

    def start(self):
        for d in self.data_replay:
            d['type'] = self.event_name
            self.listeners(d)
