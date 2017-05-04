import queue
import threading

import numpy as np

import pyiqfeed as iq
from passwords import dtn_product_id, dtn_login, dtn_password


def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = iq.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch()


def create_batch(data, key_suffix=''):
    """
    Create minibatch-type data based on the pyiqfeed data format
    :param data: data list
    :return:
    """
    for i, datum in enumerate(data):
        datum = datum[0] if len(datum) == 1 else datum

        if i == 0:
            result = {n + key_suffix: np.empty((len(data),), d.dtype if str(d.dtype) not in ('|S4', '|S3') else object) for n, d in zip(datum.dtype.names, datum)}

        for j, f in enumerate(datum.dtype.names):
            d = datum[j]
            if isinstance(datum[j], bytes):
                d = datum[j].decode('ascii')

            result[f][i] = d

    return result


def iqfeed_to_dict(data, key_suffix=''):
    """
    Turn one iqfeed data item to dict
    :param data: data list
    :param key_suffix: suffix to each name
    :return:
    """
    data = data[0] if len(data) == 1 else data

    result = {n + key_suffix: d for n, d in zip(data.dtype.names, data)}

    for k, v in result.items():
        if isinstance(v, bytes):
            result[k] = v.decode('ascii')

    return result


def adjust(data, fundamentals: dict):
    if not isinstance(data, np.ndarray):
        d = data['date']
        if d > fundamentals['Ex-dividend Date'] and d > fundamentals['Split Factor 1 Date'] and d > fundamentals['Split Factor 2 Date']:
            return

    if fundamentals['Ex-dividend Date'] > fundamentals['Split Factor 1 Date']:
        adjust_dividend(data, fundamentals['Dividend Amount'], fundamentals['Ex-dividend Date'])
        adjust_split(data, fundamentals['Split Factor 1'], fundamentals['Split Factor 1 Date'])
        adjust_split(data, fundamentals['Split Factor 2'], fundamentals['Split Factor 2 Date'])
    elif fundamentals['Split Factor 1 Date'] > fundamentals['Ex-dividend Date'] > fundamentals['Split Factor 2 Date']:
        adjust_split(data, fundamentals['Split Factor 1'], fundamentals['Split Factor 1 Date'])
        adjust_dividend(data, fundamentals['Dividend Amount'], fundamentals['Ex-dividend Date'])
        adjust_split(data, fundamentals['Split Factor 2'], fundamentals['Split Factor 2 Date'])
    elif fundamentals['Split Factor 1 Date'] > fundamentals['Split Factor 2 Date'] > fundamentals['Ex-dividend Date']:
        adjust_split(data, fundamentals['Split Factor 1'], fundamentals['Split Factor 1 Date'])
        adjust_split(data, fundamentals['Split Factor 2'], fundamentals['Split Factor 2 Date'])
        adjust_dividend(data, fundamentals['Dividend Amount'], fundamentals['Ex-dividend Date'])


def adjust_dividend(data, dividend_amount, dividend_date):
    if not np.isnan(dividend_amount):
        if isinstance(data, np.ndarray):
            if 'open_p' in data[0].dtype.names:  # adjust bars
                data['open_p'][data['date'] < dividend_date] -= dividend_amount
                data['close_p'][data['date'] < dividend_date] -= dividend_amount
                data['high_p'][data['date'] < dividend_date] -= dividend_amount
                data['low_p'][data['date'] < dividend_date] -= dividend_amount
            elif 'ask' in data[0].dtype.names:  # adjust ticks:
                data['ask'][data['date'] < dividend_date] -= dividend_amount
                data['bid'][data['date'] < dividend_date] -= dividend_amount
                data['last'][data['date'] < dividend_date] -= dividend_amount
        elif not np.isnan(dividend_amount) and dividend_date > data['date']:
            if 'open_p' in data.dtype.names:  # adjust bars
                data['open_p'] -= dividend_amount
                data['close_p'] -= dividend_amount
                data['high_p'] -= dividend_amount
                data['low_p'] -= dividend_amount
            elif 'ask' in data.dtype.names:  # adjust ticks:
                data['ask'] -= dividend_amount
                data['bid'] -= dividend_amount
                data['last'] -= dividend_amount


def adjust_split(data, split_factor, split_date):
    if not np.isnan(split_factor) and split_factor > 0:
        if isinstance(data, np.ndarray):
            if 'open_p' in data[0].dtype.names:  # adjust bars
                data['open_p'][data['date'] < split_date] *= split_factor
                data['close_p'][data['date'] < split_date] *= split_factor
                data['high_p'][data['date'] < split_date] *= split_factor
                data['low_p'][data['date'] < split_date] *= split_factor
                data['prd_vlm'][data['date'] < split_date] *= int(1 / split_factor)
                data['tot_vlm'][data['date'] < split_date] *= int(1 / split_factor)
            elif 'ask' in data[0].dtype.names:  # adjust ticks:
                data['ask'][data['date'] < split_date] *= split_factor
                data['bid'][data['date'] < split_date] *= split_factor
                data['last'][data['date'] < split_date] *= split_factor
                data['last_sz'][data['date'] < split_date] *= int(1 / split_factor)
                data['tot_vlm'][data['date'] < split_date] *= int(1 / split_factor)
        elif not np.isnan(split_factor) and split_factor > 0 and split_date > data['date']:
            if 'open_p' in data.dtype.names:  # adjust bars
                data['open_p'] *= split_factor
                data['close_p'] *= split_factor
                data['high_p'] *= split_factor
                data['low_p'] *= split_factor
                data['prd_vlm'] *= int(1 / split_factor)
                data['tot_vlm'] *= int(1 / split_factor)
            elif 'ask' in data.dtype.names:  # adjust ticks:
                data['ask'] *= split_factor
                data['bid'] *= split_factor
                data['last'] *= split_factor
                data['last_sz'] *= int(1 / split_factor)
                data['tot_vlm'] *= int(1 / split_factor)


class IQFeedDataProvider(object):
    """Streaming data provider generator/iterator interface"""

    def __init__(self, producer):
        self._queue = queue.Queue()
        self._producer = producer
        self._lock = threading.RLock()

        with self._lock:
            self._is_listening = True
            producer += self._populate_queue

    def _populate_queue(self, event):
        self._queue.put(event['data'])

    def __iter__(self):
        return self

    def __next__(self) -> map:
        return self._queue.get()

    def __enter__(self):
        with self._lock:
            if not self._is_listening:
                self._is_listening = True
                self._producer += self._populate_queue

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        with self._lock:
            if self._is_listening:
                self._is_listening = False
                self._producer -= self._populate_queue

