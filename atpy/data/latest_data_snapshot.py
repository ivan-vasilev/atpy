import threading
import typing
from collections import OrderedDict

from atpy.data.iqfeed.util import *


class LatestDataSnapshot(object):
    """Listen and maintain a dataframe of the latest data events"""

    def __init__(self, listeners, event: typing.Union[str, typing.Set[str]], depth=0, fire_update: bool = False):
        """
        :param listeners: listeners
        :param event: event or list of events to accept
        :param depth: keep depth of the snapshot
        :param fire_update: whether to fire an event in case of snapshot update
        """

        self.listeners = listeners
        self.listeners += self.on_event

        self.depth = depth
        self.event = {event} if isinstance(event, str) else event
        self._fire_update = fire_update
        self._snapshot = OrderedDict()

        self._rlock = threading.RLock()

        pd.set_option('mode.chained_assignment', 'warn')

    def update_snapshot(self, data):
        with self._rlock:
            timestamp_cols = [c for c in data if pd.core.dtypes.common.is_datetimelike(data[c])]
            if timestamp_cols:
                ind = data[timestamp_cols[0]].unique()
            else:
                if isinstance(data.index, pd.DatetimeIndex):
                    ind = data.index
                elif isinstance(data.index, pd.MultiIndex):
                    ind = data.index.levels[0]
                else:
                    ind = None

                if not isinstance(ind, pd.DatetimeIndex):
                    raise Exception("Only first level DateTimeIndex is supported")

            for i in ind[-self.depth:]:
                self._snapshot[i] = pd.concat([self._snapshot[i], data.loc[i]]) if i in self._snapshot else data.loc[i]

            while len(self._snapshot) > self.depth:
                self._snapshot.popitem()

    def on_event(self, event):
        if event['type'] in self.event:
            self.update_snapshot(event['data'])
            if self._fire_update:
                self.listeners({'type': event['type'] + '_snapshot', 'data': pd.concat(self._snapshot), 'new_data': pd.concat(self._snapshot)})
        elif event['type'] == 'request_latest':
            self.listeners({'type': 'snapshot', 'data': pd.concat(self._snapshot)})
