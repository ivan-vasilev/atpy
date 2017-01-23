import numpy as np


def create_batch(data, column_mode=True, key_suffix=''):
    """
    Create minibatch-type data based on the pyiqfeed data format
    :param data: data list
    :param column_mode: whether to convert the data to column mode (or row mode)
    :return:
    """
    if column_mode:
        for i, datum in enumerate(data):
            if i == 0:
                result = {n + key_suffix: np.empty((len(data),), d.dtype) for n, d in zip(datum.dtype.names, datum)}
            for j, f in enumerate(datum.dtype.names):
                result[f][i] = datum[j]
    else:
        result = list()
        for datum in data:
            result.append({n + key_suffix: d for n, d in zip(datum.dtype.names, datum)})

    return result
