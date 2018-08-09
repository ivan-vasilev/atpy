import functools
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd

"""
Chapter 2 of Advances in Financial Machine Learning book by Marcos Lopez de Prado
"""


def _cumsum_filter(df: pd.DataFrame):
    """
    Non multiindex cumsum filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param df: single index pd.DataFrame with 2 columns - 'value' and 'threshold'
    :return events
    """
    sneg, spos = 0, 0
    result = list()
    df[df.columns[0]] = df[df.columns[0]].diff()
    for i, v, threshold in df[1:].itertuples():
        spos, sneg = max(0, spos + v), min(0, sneg + v)
        if sneg < -threshold:
            sneg = 0
            result.append(i)
        elif spos > threshold:
            spos = 0
            result.append(i)

    if isinstance(df.index, pd.MultiIndex):
        return pd.MultiIndex.from_tuples(result, names=df.index.names)
    else:
        return pd.DatetimeIndex(result, name=df.index.name)


def cumsum_filter(df: pd.DataFrame, parallel=True):
    """
    cumsum filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param df: multi-index pd.DataFrame with 2 columns - first for the value and the second for threshold
    :param parallel: run in multiprocessing mode for multiindex dataframes
    :return events
    """
    if isinstance(df.index, pd.MultiIndex):
        grpby = df.groupby(level='symbol', group_keys=False, sort=False)
        if parallel:
            with Pool(cpu_count()) as p:
                ret_list = [pd.Series(index=x) for x in p.map(_cumsum_filter, [group for name, group in grpby])]

            return pd.concat(ret_list).index
        else:
            return grpby.apply(lambda x: pd.Series(index=_cumsum_filter(x))).index
    else:
        return _cumsum_filter(df)


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


"""
Chapter 5 of Advances in Financial Machine Learning book by Marcos Lopez de Prado
"""


def get_weights_ffd(d, threshold=1e-5):
    """
    Obtain the weights for the binomial representation of time series
    :param d: coefficient
    :param threshold: threshold
    """

    w, k = [1.], 1
    while abs(w[-1]) > threshold:
        w_ = -w[-1] / k * (d - k + 1)
        w.append(w_)
        k += 1

    w = np.array(w[::-1])

    return w


# def plot_weights(d_range, n_plots):
#     import matplotlib.pyplot as plt
#
#     w = pd.DataFrame()
#     for d in np.linspace(d_range[0], d_range[1], n_plots):
#         w_ = get_weights_ffd(d)
#         w_ = pd.DataFrame(w_, index=range(w_.shape[0])[::-1], columns=[d])
#         w = w.join(w_, how='outer')
#     ax = w.plot()
#     ax.legend(loc='upper left');
#     plt.show()
#     return
#
#
# if __name__ == '__main__':
#     plot_weights(d_range=[0, 1], n_plots=11)
#     plot_weights(d_range=[1, 2], n_plots=11)


def _frac_diff_ffd(data: pd.Series, d: float, threshold=1e-5):
    """
    Fractionally Differentiated Features Fixed Window
    :param data: data
    :param d: difference coefficient
    :param threshold: threshold
    """
    # 1) Compute weights for the longest series
    w = get_weights_ffd(d, threshold)
    # 2) Apply weights to values
    return data.rolling(w.size, min_periods=w.size).apply(lambda x: np.dot(x, w), raw=True).dropna()


def frac_diff_ffd(data: pd.Series, d: float, threshold=1e-5, parallel=False):
    """
    Fractionally Differentiated Features Fixed Window
    :param data: data
    :param d: difference coefficient
    :param threshold: threshold
    :param parallel: run in parallel
    """
    if isinstance(data.index, pd.MultiIndex):
        if parallel:
            with Pool(cpu_count()) as p:
                ret_list = p.starmap(_frac_diff_ffd, [(group, d, threshold) for name, group in data.groupby(level='symbol', group_keys=False, sort=False)])
                return pd.concat(ret_list)
        else:
            return data.groupby(level='symbol', group_keys=False, sort=False).apply(_frac_diff_ffd, d=d, threshold=threshold)
    else:
        return _frac_diff_ffd(data=data, d=d, threshold=threshold)
