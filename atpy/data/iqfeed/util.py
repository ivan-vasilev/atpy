import datetime
import logging
import os
import queue
import tempfile
import threading
import zipfile

import numpy as np
import pandas as pd
import pytz
import requests

import pyiqfeed as iq
import atpy.data.util as datautil


def dtn_credentials():
    return os.environ['DTN_PRODUCT_ID'], os.environ['DTN_LOGIN'], os.environ['DTN_PASSWORD'], 'Debugging'


def launch_service():
    """Check if IQFeed.exe is running and start if not"""
    dtn_product_id, dtn_login, dtn_password, version = dtn_credentials()

    svc = iq.FeedService(product=dtn_product_id,
                         version=version,
                         login=dtn_login,
                         password=dtn_password)

    headless = bool(os.environ["DTN_HEADLESS"]) if "DTN_HEADLESS" in os.environ else "DISPLAY" not in os.environ
    logging.getLogger(__name__).info("Launching IQFeed service in " + ("headless mode" if headless else "non headless mode"))

    svc.launch(headless=headless)


def create_batch(data, key_suffix=''):
    """
    Create minibatch-type data based on the pyiqfeed data format
    :param data: data list
    :return:
    """
    for i, datum in enumerate(data):
        datum = datum[0] if len(datum) == 1 else datum

        if i == 0:
            result = {n.replace(" ", "_").lower() + key_suffix: np.empty((len(data),), d.dtype if str(d.dtype) not in ('|S4', '|S3') else object) for n, d in zip(datum.dtype.names, datum)}

        for j, f in enumerate(datum.dtype.names):
            d = datum[j]
            if isinstance(datum[j], bytes):
                d = datum[j].decode('ascii')

            result[f.replace(" ", "_").lower()][i] = d

    return result


def iqfeed_to_dict(data, key_suffix=''):
    """
    Turn one iqfeed data item to dict
    :param data: data list
    :param key_suffix: suffix to each name
    :return:
    """
    data = data[0] if len(data) == 1 else data

    result = {n.replace(" ", "_").lower() + key_suffix: d for n, d in zip(data.dtype.names, data)}

    for k, v in result.items():
        if isinstance(v, bytes):
            result[k] = v.decode('ascii')

    return result


def adjust(data, fundamentals: dict):
    if not isinstance(data, pd.DataFrame):
        d = data['timestamp'].date()
        if d > fundamentals['ex-dividend_date'] and d > fundamentals['split_factor_1_date'] and d > fundamentals['split_factor_2_date']:
            return

    fundamentals = {k: None if pd.isnull(v) else v for k, v in fundamentals.items()}
    symbol = fundamentals['symbol']
    datautil.adjust(symbol=symbol,
                    data=data,
                    splits=[(fundamentals['split_factor_1_date'], fundamentals['split_factor_1']), (fundamentals['split_factor_2_date'], fundamentals['split_factor_2'])],
                    dividends=[(fundamentals['ex-dividend_date'], fundamentals['dividend_amount'])])


def get_symbols(symbols_file: str = None):
    with tempfile.TemporaryDirectory() as td:
        if symbols_file is not None:
            logging.getLogger(__name__).info("Symbols: " + symbols_file)
            zipfile.ZipFile(symbols_file).extractall(td)
        else:
            with tempfile.TemporaryFile() as tf:
                logging.getLogger(__name__).info("Downloading symbol list... ")
                tf.write(requests.get('http://www.dtniq.com/product/mktsymbols_v2.zip', allow_redirects=True).content)
                zipfile.ZipFile(tf).extractall(td)

        with open(os.path.join(td, 'mktsymbols_v2.txt')) as f:
            content = f.readlines()

    content = [c for c in content if '\tEQUITY' in c and ('\tNYSE' in c or '\tNASDAQ' in c)]
    return {s.split('\t')[0] for s in content}


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
