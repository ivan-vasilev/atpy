import functools
from multiprocessing import Pool, cpu_count

import numba
import numpy as np
import pandas as pd

"""
Chapter 2 of Advances in Financial Machine Learning book by Marcos Lopez de Prado
"""


def cumsum_filter(values: pd.Series, thresholds: pd.Series, parallel=True):
    """
    cumsum filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param values: series of values to apply the cumsum filter over
    :param thresholds: series of thresholds for the values of the cumsum filter
    :param parallel: run in multiprocessing mode for multiindex dataframes
    :return events
    """
    if values.index.equals(thresholds.index) is False:
        raise ValueError('values and thresholds have different index')

    df = pd.concat([values.rename('value'), thresholds.rename('threshold')], axis=1)

    if isinstance(df.index, pd.MultiIndex):
        grpby = df.groupby(level='symbol', group_keys=False, sort=False)
        if parallel:
            with Pool(cpu_count()) as p:
                ret_list = p.map(_cumsum_filter, [group for name, group in grpby])

            return pd.concat(ret_list).index
        else:
            return grpby.apply(_cumsum_filter).index
    else:
        return _cumsum_filter(df).index


def _cumsum_filter(df: pd.DataFrame):
    """
    Non multiindex cumsum filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param df: single index pd.DataFrame with 2 columns - 'value' and 'threshold'
    :return events
    """

    if isinstance(df.index, pd.MultiIndex):
        symbol_ind = df.index.names.index('symbol')
        result = __cumsum_filter(df.index.droplevel(symbol_ind).values,
                                 df['value'].values,
                                 df['threshold'].values)

        result = pd.DatetimeIndex(result, tz=df.index.levels[df.index.names.index('timestamp')].tz)

        return pd.Series(index=pd.MultiIndex.from_product(
            [[df.index[0][symbol_ind]], result] if symbol_ind == 0 else [result, [df.index[0][symbol_ind]]],
            names=df.index.names
        ))
    else:
        result = __cumsum_filter(df.index.values,
                                 df['value'].values,
                                 df['threshold'].values)

        result = pd.DatetimeIndex(result, tz=df.index.tz)

        return pd.Series(index=pd.DatetimeIndex(result, name=df.index.name))


@numba.jit(nopython=True)
def __cumsum_filter(timestamps: np.array, values: np.array, thresholds: np.array):
    """
    Non multiindex cumsum filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param timestamps: timestamps numpy array
    :param values: the actual values of the time series
    :param thresholds: threshold for each value
    :return events
    """
    sneg, spos = 0, 0
    result = []
    values = np.diff(values)
    for i, v, threshold in zip(timestamps[1:], values, thresholds[1:]):
        spos, sneg = max(0, spos + v), min(0, sneg + v)
        if sneg < -threshold:
            sneg = 0
            result.append(i)
        elif spos > threshold:
            spos = 0
            result.append(i)

    return result


def _daily_volatility(price: pd.Series, span=100):
    """
    Compute daily volatility  filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param price: single index pd.Series
    :param span: number of days to include in the volatility computation
    :return events
    """
    tmp = price.index.searchsorted(price.index - pd.Timedelta(days=1))
    tmp = tmp[tmp > 0]
    tmp = price.index[tmp - 1].to_series(keep_tz=True, index=price.index[price.shape[0] - tmp.shape[0]:])
    tmp = price.loc[tmp.index] / price.loc[tmp.values].values - 1
    tmp = tmp.ewm(span=span).std().dropna()

    return tmp


def _daily_volatility_mi(price: pd.Series, span=100):
    if isinstance(price.index, pd.MultiIndex):
        symbol_ind = price.index.names.index('symbol')
        symbol = price.index[0][symbol_ind]
        price = price.loc[pd.IndexSlice[:, symbol]] if symbol_ind == 1 else price.loc[symbol]
    else:
        symbol_ind, symbol = -1, None

    tmp = _daily_volatility(price=price, span=span)

    if symbol_ind > -1:
        tmp = tmp.to_frame()
        tmp['symbol'] = symbol
        tmp.set_index('symbol', append=True, inplace=True, drop=True)
        if symbol_ind == 0:
            tmp = tmp.reorder_levels(['symbol', 'timestamp'])

        tmp = tmp[tmp.columns[0]]

    return tmp


def daily_volatility(price: pd.Series, span=100, parallel=True):
    """
    Compute daily volatility  filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param price: single index pd.Series
    :param span: number of days to include in the volatility computation
    :param parallel: run in multiprocessing mode for multiindex dataframes
    :return events
    """
    if isinstance(price.index, pd.MultiIndex):
        if parallel:
            with Pool(cpu_count()) as p:
                ret_list = p.starmap(_daily_volatility_mi, [(group, span) for name, group in price.groupby(level='symbol', group_keys=False, sort=False)])

            result = pd.concat(ret_list)
        else:
            result = price.groupby(level='symbol', group_keys=False, sort=False).apply(functools.partial(_daily_volatility_mi, span=span))
    else:
        result = _daily_volatility(price)

    return result
