import queue
import threading
import datetime

import numpy as np
import pandas as pd

import pyiqfeed as iq
import pytz
import os
import logging


def dtn_credentials():
    return os.environ['DTN_PRODUCT_ID'], os.environ['DTN_LOGIN'], os.environ['DTN_PASSWORD'], 'Debugging'


def launch_service():
    """Check if IQFeed.exe is running and start if not"""
    dtn_product_id, dtn_login, dtn_password, version = dtn_credentials()

    svc = iq.FeedService(product=dtn_product_id,
                         version=version,
                         login=dtn_login,
                         password=dtn_password)

    headless = "DISPLAY" not in os.environ
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

    symbol = fundamentals['symbol']

    if fundamentals['ex-dividend_date'] > fundamentals['split_factor_1_date']:
        adjust_dividend(data, symbol, fundamentals['dividend_amount'], fundamentals['ex-dividend_date'])
        adjust_split(data, symbol, fundamentals['split_factor_1'], fundamentals['split_factor_1_date'])
        adjust_split(data, symbol, fundamentals['split_factor_2'], fundamentals['split_factor_2_date'])
    elif fundamentals['split_factor_1_date'] > fundamentals['ex-dividend_date'] > fundamentals['split_factor_2_date']:
        adjust_split(data, symbol, fundamentals['split_factor_1'], fundamentals['split_factor_1_date'])
        adjust_dividend(data, symbol, fundamentals['dividend_amount'], fundamentals['ex-dividend_date'])
        adjust_split(data, symbol, fundamentals['split_factor_2'], fundamentals['split_factor_2_date'])
    elif fundamentals['split_factor_1_date'] > fundamentals['split_factor_2_date'] > fundamentals['ex-dividend_date']:
        adjust_split(data, symbol, fundamentals['split_factor_1'], fundamentals['split_factor_1_date'])
        adjust_split(data, symbol, fundamentals['split_factor_2'], fundamentals['split_factor_2_date'])
        adjust_dividend(data, symbol, fundamentals['dividend_amount'], fundamentals['ex-dividend_date'])


def adjust_dividend(data, symbol, dividend_amount, dividend_date):
    if not np.isnan(dividend_amount):
        if isinstance(data, pd.DataFrame):
            if 'open' in data.columns:  # adjust bars
                if isinstance(data.index, pd.MultiIndex):
                    timezone = pytz.timezone(data.index.levels[1].tz.zone)
                    dividend_date = timezone.localize(datetime.datetime.combine(dividend_date.astype(datetime.datetime), datetime.datetime.min.time()))
                    mask = data['timestamp'] < dividend_date
                    mask[data['symbol'] != symbol] = False

                    data.loc[mask, 'close'] -= dividend_amount
                    data.loc[mask, 'high'] -= dividend_amount
                    data.loc[mask, 'open'] -= dividend_amount
                    data.loc[mask, 'low'] -= dividend_amount
                else:
                    data.loc[data.index < dividend_date, 'close'] -= dividend_amount
                    data.loc[data.index < dividend_date, 'high'] -= dividend_amount
                    data.loc[data.index < dividend_date, 'open'] -= dividend_amount
                    data.loc[data.index < dividend_date, 'low'] -= dividend_amount
            elif 'ask' in data.columns:  # adjust ticks:
                data.loc[data['timestamp'] < dividend_date, 'ask'] -= dividend_amount
                data.loc[data['timestamp'] < dividend_date, 'bid'] -= dividend_amount
                data.loc[data['timestamp'] < dividend_date, 'last'] -= dividend_amount
        elif not np.isnan(dividend_amount) and dividend_date > data['timestamp']:
            if 'open' in data:  # adjust bars
                data['open'] -= dividend_amount
                data['close'] -= dividend_amount
                data['high'] -= dividend_amount
                data['low'] -= dividend_amount
            elif 'ask' in data:  # adjust ticks:
                data['ask'] -= dividend_amount
                data['bid'] -= dividend_amount
                data['last'] -= dividend_amount


def adjust_split(data, symbol, split_factor, split_date):
    if not np.isnan(split_factor) and split_factor > 0:
        if isinstance(data, pd.DataFrame):
            if 'open' in data.columns:  # adjust bars
                if isinstance(data.index, pd.MultiIndex):
                    timezone = pytz.timezone(data.index.levels[1].tz.zone)
                    split_date = timezone.localize(datetime.datetime.combine(split_date.astype(datetime.datetime), datetime.datetime.min.time()))
                    mask = data['timestamp'] < split_date
                    mask[data['symbol'] != symbol] = False

                    data.loc[mask, 'open'] *= split_factor
                    data.loc[mask, 'close'] *= split_factor
                    data.loc[mask, 'high'] *= split_factor
                    data.loc[mask, 'low'] *= split_factor
                    data.loc[mask, 'period_volume'] *= int(1 / split_factor)
                    data.loc[mask, 'total_volume'] *= int(1 / split_factor)
                else:
                    data.loc[data.index < split_date, 'open'] *= split_factor
                    data.loc[data.index < split_date, 'close'] *= split_factor
                    data.loc[data.index < split_date, 'high'] *= split_factor
                    data.loc[data.index < split_date, 'low'] *= split_factor
                    data.loc[data.index < split_date, 'period_volume'] *= int(1 / split_factor)
                    data.loc[data.index < split_date, 'total_volume'] *= int(1 / split_factor)
            elif 'ask' in data.columns:  # adjust ticks:
                data.loc[data['timestamp'] < split_date, 'ask'] *= split_factor
                data.loc[data['timestamp'] < split_date, 'bid'] *= split_factor
                data.loc[data['timestamp'] < split_date, 'last'] *= split_factor
                data.loc[data['timestamp'] < split_date, 'last_size'] *= int(1 / split_factor)
                data.loc[data['timestamp'] < split_date, 'total_volume'] *= int(1 / split_factor)
        elif not np.isnan(split_factor) and split_factor > 0 and split_date > data['timestamp']:
            if 'open' in data:  # adjust bars
                data['open'] *= split_factor
                data['close'] *= split_factor
                data['high'] *= split_factor
                data['low'] *= split_factor
                data['period_volume'] *= int(1 / split_factor)
                data['total_volume'] *= int(1 / split_factor)
            elif 'ask' in data:  # adjust ticks:
                data['ask'] *= split_factor
                data['bid'] *= split_factor
                data['last'] *= split_factor
                data['last_size'] *= int(1 / split_factor)
                data['total_volume'] *= int(1 / split_factor)


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
