import datetime
import logging
import typing

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

        for (iterator, name, historical_depth, listeners) in self._sources_defs:
            sources[name] = (iter(iterator), historical_depth, listeners)

        self._sources = sources

        return self

    def __next__(self):
        # delete "expired" dataframes and obtain new data from the providers
        for e, (dp, historical_depth, listeners) in dict(self._sources).items():
            if e not in self._data or \
                    (self._current_time is not None and self._get_datetime_level(self._data[e].index)[-1] <= self._current_time):
                self._timeline = None

                now = datetime.datetime.now()
                try:
                    df = next(dp)
                    while df is not None and df.empty:
                        df = next(dp)
                except StopIteration:
                    df = None

                if df is not None:
                    logging.getLogger(__name__).debug('Obtained data ' + str(e) + ' in ' + str(datetime.datetime.now() - now))

                    # prepend old data if exists
                    if e in self._data and historical_depth > 0:
                        ind = self._get_datetime_level(self._data[e])
                        old_df_slice = self._data[e].loc[slice(ind[max(-len(ind), -historical_depth)], ind[-1]), :]
                        df = pd.concat((old_df_slice, df))

                    self._data[e] = df

                    if listeners is not None:
                        listeners({'type': 'pre_data', e + '_full': df})
                else:
                    if e in self._data:
                        del self._data[e]

                    del self._sources[e]

        # build timeline
        if self._timeline is None and self._data:
            now = datetime.datetime.now()

            indices = [self._get_datetime_level(df.index) for df in self._data.values()]
            tzs = {ind.tz for ind in indices}

            if len(tzs) > 1:
                raise Exception("Multiple timezones detected")

            ind = indices[0].union_many(indices[1:]).unique().sort_values()

            self._timeline = pd.DataFrame(index=ind)

            for e, df in self._data.items():
                ind = self._get_datetime_level(df.index)
                self._timeline[e] = False
                self._timeline.loc[ind, e] = True

            logging.getLogger(__name__).debug('Built timeline in ' + str(datetime.datetime.now() - now))

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

            result['timestamp'] = self._current_time.to_pydatetime()

            row = self._timeline.iloc[current_index]

            for e in [e for e in row.index if row[e]]:
                df = self._data[e]
                _, historical_depth, _ = self._sources[e]
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

    def add_source(self, data_provider: typing.Union[typing.Iterator, typing.Callable], name: str, historical_depth: int = 0, listeners: typing.Callable = None):
        """
        :param data_provider: return pd.DataFrame with either DateTimeIndex or MultiIndex, where one of the levels is of datetime type
        :param name: data set name for each of the data sources
        :param historical_depth: whether to return only the current element or with historical depth
        :param listeners: Fire event after each data provider request.
                This is necessary, because the data replay functionality is combining the new/old dataframes for continuity.
                Process data, once obtained from the data provider (applied once for the whole chunk).
        :return: self
        """
        if self._is_running:
            raise Exception("Cannot add sources while the generator is working")

        self._sources_defs.append((data_provider, name, historical_depth, listeners))

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
