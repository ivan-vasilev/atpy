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


def create_batch(data, column_mode=True, key_suffix=''):
    """
    Create minibatch-type data based on the pyiqfeed data format
    :param data: data list
    :param column_mode: whether to convert the data to column mode (or row mode)
    :return:
    """
    if column_mode:
        for i, datum in enumerate(data):
            if len(datum) == 1:
                datum = datum[0]

            if i == 0:
                result = {n + key_suffix: np.empty((len(data),), d.dtype) for n, d in zip(datum.dtype.names, datum)}

            for j, f in enumerate(datum.dtype.names):
                result[f][i] = datum[j]
    else:
        result = list()
        for datum in data:
            result.append(iqfeed_to_dict(datum, key_suffix))

    return result


def iqfeed_to_dict(data, key_suffix=''):
    """
    Turn one iqfeed data item to dict
    :param data: data list
    :param key_suffix: suffix to each name
    :return:
    """
    if len(data) == 1:
        data = data[0]

    return {n + key_suffix: d for n, d in zip(data.dtype.names, data)}
