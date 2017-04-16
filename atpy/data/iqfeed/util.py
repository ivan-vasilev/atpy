import numpy as np
import pyiqfeed as iq
from passwords import dtn_product_id, dtn_login, dtn_password
import queue
import threading
import pandas as pd


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


def adjust_bars(bar_data, fundamentals: dict):
    d = bar_data['date']

    if d > fundamentals['Ex-dividend Date'] and d > fundamentals['Split Factor 1 Date'] and d > fundamentals['Split Factor 2 Date']:
        return

    if fundamentals['Ex-dividend Date'] > d > fundamentals['Split Factor 1 Date']:
        if not np.isnan(fundamentals['Dividend Amount']):
            bar_data['open_p'] -= fundamentals['Dividend Amount']
            bar_data['close_p'] -= fundamentals['Dividend Amount']
            bar_data['high_p'] -= fundamentals['Dividend Amount']
            bar_data['low_p'] -= fundamentals['Dividend Amount']
    elif fundamentals['Ex-dividend Date'] > fundamentals['Split Factor 1 Date'] > d > fundamentals['Split Factor 2 Date']:
        if not np.isnan(fundamentals['Dividend Amount']):
            bar_data['open_p'] -= fundamentals['Dividend Amount']
            bar_data['close_p'] -= fundamentals['Dividend Amount']
            bar_data['high_p'] -= fundamentals['Dividend Amount']
            bar_data['low_p'] -= fundamentals['Dividend Amount']

        if not np.isnan(fundamentals['Split Factor 1']):
            bar_data['open_p'] /= fundamentals['Split Factor 1']
            bar_data['close_p'] /= fundamentals['Split Factor 1']
            bar_data['high_p'] /= fundamentals['Split Factor 1']
            bar_data['low_p'] /= fundamentals['Split Factor 1']
            bar_data['prd_vlm'] *= fundamentals['Split Factor 1']
            bar_data['tot_vlm'] *= fundamentals['Split Factor 1']
    elif fundamentals['Ex-dividend Date'] > fundamentals['Split Factor 1 Date'] > fundamentals['Split Factor 2 Date'] > d:
        if not np.isnan(fundamentals['Dividend Amount']):
            bar_data['open_p'] -= fundamentals['Dividend Amount']
            bar_data['close_p'] -= fundamentals['Dividend Amount']
            bar_data['high_p'] -= fundamentals['Dividend Amount']
            bar_data['low_p'] -= fundamentals['Dividend Amount']

        if not np.isnan(fundamentals['Split Factor 1']):
            bar_data['open_p'] /= fundamentals['Split Factor 1']
            bar_data['close_p'] /= fundamentals['Split Factor 1']
            bar_data['high_p'] /= fundamentals['Split Factor 1']
            bar_data['low_p'] /= fundamentals['Split Factor 1']
            bar_data['prd_vlm'] *= fundamentals['Split Factor 1']
            bar_data['tot_vlm'] *= fundamentals['Split Factor 1']

        if not np.isnan(fundamentals['Split Factor 2']):
            bar_data['open_p'] /= fundamentals['Split Factor 2']
            bar_data['close_p'] /= fundamentals['Split Factor 2']
            bar_data['high_p'] /= fundamentals['Split Factor 2']
            bar_data['low_p'] /= fundamentals['Split Factor 2']
            bar_data['prd_vlm'] *= fundamentals['Split Factor 2']
            bar_data['tot_vlm'] *= fundamentals['Split Factor 2']
    elif fundamentals['Split Factor 1 Date'] > d > fundamentals['Ex-dividend Date']:
        if not np.isnan(fundamentals['Split Factor 1']):
            bar_data['open_p'] /= fundamentals['Split Factor 1']
            bar_data['close_p'] /= fundamentals['Split Factor 1']
            bar_data['high_p'] /= fundamentals['Split Factor 1']
            bar_data['low_p'] /= fundamentals['Split Factor 1']
            bar_data['prd_vlm'] *= fundamentals['Split Factor 1']
            bar_data['tot_vlm'] *= fundamentals['Split Factor 1']
    elif fundamentals['Split Factor 1 Date'] > fundamentals['Ex-dividend Date'] > d > fundamentals['Split Factor 2 Date']:
        if not np.isnan(fundamentals['Split Factor 1']):
            bar_data['open_p'] /= fundamentals['Split Factor 1']
            bar_data['close_p'] /= fundamentals['Split Factor 1']
            bar_data['high_p'] /= fundamentals['Split Factor 1']
            bar_data['low_p'] /= fundamentals['Split Factor 1']
            bar_data['prd_vlm'] *= fundamentals['Split Factor 1']
            bar_data['tot_vlm'] *= fundamentals['Split Factor 1']

        if not np.isnan(fundamentals['Dividend Amount']):
            bar_data['open_p'] -= fundamentals['Dividend Amount']
            bar_data['close_p'] -= fundamentals['Dividend Amount']
            bar_data['high_p'] -= fundamentals['Dividend Amount']
            bar_data['low_p'] -= fundamentals['Dividend Amount']
    elif fundamentals['Split Factor 1 Date'] > fundamentals['Split Factor 2 Date'] > fundamentals['Ex-dividend Date'] > d:
        if not np.isnan(fundamentals['Split Factor 1']):
            bar_data['open_p'] /= fundamentals['Split Factor 1']
            bar_data['close_p'] /= fundamentals['Split Factor 1']
            bar_data['high_p'] /= fundamentals['Split Factor 1']
            bar_data['low_p'] /= fundamentals['Split Factor 1']
            bar_data['prd_vlm'] *= fundamentals['Split Factor 1']
            bar_data['tot_vlm'] *= fundamentals['Split Factor 1']

        if not np.isnan(fundamentals['Split Factor 2']):
            bar_data['open_p'] /= fundamentals['Split Factor 2']
            bar_data['close_p'] /= fundamentals['Split Factor 2']
            bar_data['high_p'] /= fundamentals['Split Factor 2']
            bar_data['low_p'] /= fundamentals['Split Factor 2']
            bar_data['prd_vlm'] *= fundamentals['Split Factor 2']
            bar_data['tot_vlm'] *= fundamentals['Split Factor 2']

        if not np.isnan(fundamentals['Dividend Amount']):
            bar_data['open_p'] -= fundamentals['Dividend Amount']
            bar_data['close_p'] -= fundamentals['Dividend Amount']
            bar_data['high_p'] -= fundamentals['Dividend Amount']
            bar_data['low_p'] -= fundamentals['Dividend Amount']


def adjust_ticks(tick, fundamentals: dict):
    d = tick['date']

    if d > fundamentals['Ex-dividend Date'] and d > fundamentals['Split Factor 1 Date'] and d > fundamentals['Split Factor 2 Date']:
        return

    if fundamentals['Ex-dividend Date'] > d > fundamentals['Split Factor 1 Date']:
        if not np.isnan(fundamentals['Dividend Amount']):
            tick['ask'] -= fundamentals['Dividend Amount']
            tick['bid'] -= fundamentals['Dividend Amount']
            tick['last'] -= fundamentals['Dividend Amount']
    elif fundamentals['Ex-dividend Date'] > fundamentals['Split Factor 1 Date'] > d > fundamentals['Split Factor 2 Date']:
        if not np.isnan(fundamentals['Dividend Amount']):
            tick['ask'] -= fundamentals['Dividend Amount']
            tick['bid'] -= fundamentals['Dividend Amount']
            tick['last'] -= fundamentals['Dividend Amount']

        if not np.isnan(fundamentals['Split Factor 1']):
            tick['ask'] /= fundamentals['Split Factor 1']
            tick['bid'] /= fundamentals['Split Factor 1']
            tick['last'] /= fundamentals['Split Factor 1']
            tick['last_sz'] *= fundamentals['Split Factor 1']
            tick['tot_vlm'] *= fundamentals['Split Factor 1']
    elif fundamentals['Ex-dividend Date'] > fundamentals['Split Factor 1 Date'] > fundamentals['Split Factor 2 Date'] > d:
        if not np.isnan(fundamentals['Dividend Amount']):
            tick['ask'] -= fundamentals['Dividend Amount']
            tick['bid'] -= fundamentals['Dividend Amount']
            tick['last'] -= fundamentals['Dividend Amount']

        if not np.isnan(fundamentals['Split Factor 1']):
            tick['ask'] /= fundamentals['Split Factor 1']
            tick['bid'] /= fundamentals['Split Factor 1']
            tick['last'] /= fundamentals['Split Factor 1']
            tick['last_sz'] *= fundamentals['Split Factor 1']
            tick['tot_vlm'] *= fundamentals['Split Factor 1']

        if not np.isnan(fundamentals['Split Factor 2']):
            tick['ask'] /= fundamentals['Split Factor 2']
            tick['bid'] /= fundamentals['Split Factor 2']
            tick['last'] /= fundamentals['Split Factor 2']
            tick['last_sz'] *= fundamentals['Split Factor 2']
            tick['tot_vlm'] *= fundamentals['Split Factor 2']
    elif fundamentals['Split Factor 1 Date'] > d > fundamentals['Ex-dividend Date']:
        if not np.isnan(fundamentals['Split Factor 1']):
            tick['ask'] /= fundamentals['Split Factor 1']
            tick['bid'] /= fundamentals['Split Factor 1']
            tick['last'] /= fundamentals['Split Factor 1']
            tick['last_sz'] *= fundamentals['Split Factor 1']
            tick['tot_vlm'] *= fundamentals['Split Factor 1']
    elif fundamentals['Split Factor 1 Date'] > fundamentals['Ex-dividend Date'] > d > fundamentals['Split Factor 2 Date']:
        if not np.isnan(fundamentals['Split Factor 1']):
            tick['ask'] /= fundamentals['Split Factor 1']
            tick['bid'] /= fundamentals['Split Factor 1']
            tick['last'] /= fundamentals['Split Factor 1']
            tick['last_sz'] *= fundamentals['Split Factor 1']
            tick['tot_vlm'] *= fundamentals['Split Factor 1']

        if not np.isnan(fundamentals['Dividend Amount']):
            tick['ask'] -= fundamentals['Dividend Amount']
            tick['bid'] -= fundamentals['Dividend Amount']
            tick['last'] -= fundamentals['Dividend Amount']
    elif fundamentals['Split Factor 1 Date'] > fundamentals['Split Factor 2 Date'] > fundamentals['Ex-dividend Date'] > d:
        if not np.isnan(fundamentals['Split Factor 1']):
            tick['ask'] /= fundamentals['Split Factor 1']
            tick['bid'] /= fundamentals['Split Factor 1']
            tick['last'] /= fundamentals['Split Factor 1']
            tick['last_sz'] *= fundamentals['Split Factor 1']
            tick['tot_vlm'] *= fundamentals['Split Factor 1']

        if not np.isnan(fundamentals['Split Factor 2']):
            tick['ask'] /= fundamentals['Split Factor 2']
            tick['bid'] /= fundamentals['Split Factor 2']
            tick['last'] /= fundamentals['Split Factor 2']
            tick['last_sz'] *= fundamentals['Split Factor 2']
            tick['tot_vlm'] *= fundamentals['Split Factor 2']

        if not np.isnan(fundamentals['Dividend Amount']):
            tick['ask'] -= fundamentals['Dividend Amount']
            tick['bid'] -= fundamentals['Dividend Amount']
            tick['last'] -= fundamentals['Dividend Amount']


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

