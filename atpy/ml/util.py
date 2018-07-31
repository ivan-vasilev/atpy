import functools
import typing
from multiprocessing import Pool, cpu_count

import numba
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


"""
Chapter 3 of Advances in Financial Machine Learning book by Marcos Lopez de Prado
"""


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


def _vertical_barrier(timestamps: typing.Union[pd.DatetimeIndex, pd.MultiIndex], t_events: typing.Union[pd.DatetimeIndex, pd.MultiIndex], delta: pd.Timedelta):
    """
    Compute the vertical barrier from the triple barrier labeling method based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param timestamps: index of timestamps that represent bars
    :param t_events: index of timestamps that represent interesting points in the above timestamps
    :param delta: width (in time) of the label (the distance between beginning and the end of the label)
    :return events
    """
    tmp = timestamps.searchsorted(t_events + delta)
    tmp = tmp[tmp < timestamps.shape[0]]
    tmp = pd.Series(timestamps[tmp], index=t_events[:tmp.shape[0]], name='t_end').to_frame().set_index('t_end', append=True, drop=True).index

    return tmp


def vertical_barrier(timestamps: typing.Union[pd.DatetimeIndex, pd.MultiIndex], t_events: typing.Union[pd.DatetimeIndex, pd.MultiIndex], delta: pd.Timedelta):
    """
    For multiindex Dataframes (symbol and timestamp): Compute the vertical barrier from the triple barrier labeling method based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param timestamps: index of timestamps that represent bars
    :param t_events: index of timestamps that represent interesting points in the above timestamps
    :param delta: width (in time) of the label (the distance between beginning and the end of the label)
    :return events
    """
    if isinstance(timestamps, pd.MultiIndex):
        def vertical_barrier_tmp(_timestamps, _t_events):
            _timestamps = _timestamps.index

            if isinstance(_t_events, pd.MultiIndex):
                symbol_ind = _timestamps.names.index('symbol')
                symbol = _timestamps[0][symbol_ind]
                _t_events = pd.Series(index=_t_events).loc[pd.IndexSlice[:, symbol]].index if symbol_ind == 1 else pd.Series(index=_t_events).loc[symbol].index

            if isinstance(_timestamps, pd.MultiIndex):
                symbol_ind = _timestamps.names.index('symbol')
                symbol = _timestamps[0][symbol_ind]
                _timestamps = pd.Series(index=_timestamps).loc[pd.IndexSlice[:, symbol]].index if symbol_ind == 1 else pd.Series(index=_timestamps).loc[symbol].index
            else:
                symbol_ind, symbol = -1, None

            tmp = pd.DataFrame(index=_vertical_barrier(timestamps=_timestamps, t_events=_t_events, delta=delta))

            if symbol_ind > -1:
                tmp['symbol'] = symbol
                tmp.set_index('symbol', append=True, inplace=True, drop=True)
                if symbol_ind == 0:
                    tmp = tmp.reorder_levels(['symbol', 'timestamp', 't_end'])
                elif symbol_ind == 1:
                    tmp = tmp.reorder_levels(['timestamp', 't_end', 'symbol'])

            return pd.DataFrame(index=tmp.index)

        vertical_barrier_tmp = functools.partial(vertical_barrier_tmp, _t_events=t_events)

        return pd.DataFrame(index=timestamps).groupby(level='symbol', group_keys=False, sort=False).apply(vertical_barrier_tmp).index
    else:
        return _vertical_barrier(timestamps=timestamps, t_events=t_events, delta=delta)


def _triple_barriers(data: pd.Series, pt: pd.Series, sl: pd.Series, vb: pd.Timedelta, side: pd.Series = None):
    """
    Triple barrier labeling for single index series
    :param data: data to be labeled
    :param vb: vertical barrier delta
    :param pt: profit taking barrier
    :param sl: stop loss barrier
    :param side: if side is specified, then meta-labeling is enabled
    :return dataframe with first timestamp of threshold crossing, type of threshold crossing and return
    """
    result = data.to_frame()
    result['barrier_hit'] = data.index.copy()
    result['barrier_hit'].values.fill(np.timedelta64('NaT'))
    result['returns'] = data.copy()
    result['returns'].values.fill(np.nan)

    result.drop(data.name, axis=1, inplace=True)

    if side is None:
        result['side'] = data.copy().astype(np.int8)
        result['side'].values.fill(np.nan)

        __triple_barriers_side_jit(data=data.values, timestamps=data.index.values, delta=vb.to_timedelta64(), pt=pt.values, sl=sl.values, barrier_hit=result['barrier_hit'].values, side=result['side'].values,
                                   returns=result['returns'].values)
    else:
        result['size'] = data.copy().astype(np.int8)
        result['size'].values.fill(np.nan)

        __triple_barriers_size_jit(data=data.values, timestamps=data.index.values, delta=vb.to_timedelta64(), pt=pt.values, sl=sl.values, barrier_hit=result['barrier_hit'].values, side=side.values, returns=result['returns'].values,
                                   size=result['size'].values)

    return result


@numba.jit(nopython=True)
def __triple_barriers_side_jit(data: np.array, timestamps: np.array, delta: np.timedelta64, pt: np.array, sl: np.array, barrier_hit: np.array, side: np.array, returns: np.array):
    """This function cannot be local to _triple_barriers, because it will be compiled on every groupby"""

    for i in range(data.size):
        end = timestamps[i] + delta

        current = data[i]

        pt_delta = pt[i]
        sl_delta = -sl[i]

        for i_end in range(i + 1, data.size):
            ret = current / data[i_end] - 1

            if ret > pt_delta or ret < sl_delta:
                barrier_hit[i], side[i], returns[i] = timestamps[i_end], np.sign(ret), ret
                break

            if timestamps[i_end] == end or (i_end < data.size - 1 and timestamps[i_end + 1] > end):
                barrier_hit[i], side[i], returns[i] = timestamps[i_end], 0, ret
                break


@numba.jit(nopython=True)
def __triple_barriers_size_jit(data: np.array, timestamps: np.array, delta: np.timedelta64, pt: np.array, sl: np.array, barrier_hit: np.array, side: np.array, returns: np.array, size: np.array):
    """This function cannot be local to _triple_barriers, because it will be compiled on every groupby"""

    for i in range(data.size):
        end = timestamps[i] + delta

        current = data[i]

        pt_delta = pt[i]
        sl_delta = -sl[i]

        for i_end in range(i + 1, data.size):
            ret = current / data[i_end] - 1

            if ret > pt_delta or ret < sl_delta:
                barrier_hit[i], returns[i] = timestamps[i_end], ret
                size[i] = 1 if np.sign(ret) == side[i] else 0
                break

            if timestamps[i_end] == end or (i_end < data.size - 1 and timestamps[i_end + 1] > end):
                barrier_hit[i], size[i], returns[i] = timestamps[i_end], 0, ret
                break


def triple_barriers(data: pd.Series, pt: pd.Series, sl: pd.Series, vb: pd.Timedelta, side: pd.Series = None, parallel=True):
    """
    Triple barrier labeling for single multiindex series
    :param data: data to be labeled
    :param vb: vertical barrier delta
    :param pt: profit taking barrier
    :param sl: stop loss barrier
    :param side: if side is specified, then meta-labeling is enabled
    :param parallel: run in parallel
    :return dataframe with first timestamp of threshold crossing, type of threshold crossing and return
    """
    if isinstance(data.index, pd.DatetimeIndex):
        return _triple_barriers(data=data, pt=pt, sl=sl, side=side, vb=vb)
    else:
        data = data.astype(np.float32).to_frame().rename(columns={data.name: 'data'}) if data.dtype != np.float32 else data.to_frame().rename(columns={data.name: 'data'})

        if pt is not None:
            data['pt'] = pt.astype(np.float32) if pt.dtype != np.float32 else pt

        if sl is not None:
            data['sl'] = sl.astype(np.float32) if sl.dtype != np.float32 else sl

        if side is not None:
            data['side'] = side.astype(np.int8) if side.dtype != np.int8 else side

        if parallel:
            with Pool(cpu_count()) as p:
                ret_list = p.starmap(__triple_barriers_groupby, [(group, vb) for name, group in data.groupby(level='symbol', group_keys=False, sort=False)])
                return pd.concat(ret_list)
        else:
            return data.groupby(level='symbol', group_keys=False, sort=False).apply(__triple_barriers_groupby, vb=vb)


def __triple_barriers_groupby(data: pd.DataFrame, vb: pd.Timedelta):
    """
    Triple barrier labeling for single index series, specific for groupby
    :param data: dataframe with 3 (or 4) columns - the data itself, pt (profit take) and sl (stop loss) to be labeled.
        If 'side' is specified, then meta-labeling is enabled
    :param vb: vertical barrier delta
    :return dataframe with first timestamp of threshold crossing, type of threshold crossing and return
    """
    if isinstance(data.index, pd.MultiIndex):
        symbol_ind = data.index.names.index('symbol')
        symbol = data.index[0][symbol_ind]
        data = data.loc[pd.IndexSlice[:, symbol]] if symbol_ind == 1 else data.loc[symbol]
    else:
        symbol_ind, symbol = -1, None

    result = _triple_barriers(data['data'], pt=data['pt'] if 'pt' in data else None, sl=data['sl'] if 'sl' in data else None, vb=vb, side=data['side'] if 'side' in data else None)

    if symbol_ind > -1:
        result['symbol'] = symbol
        result.set_index('symbol', append=True, inplace=True, drop=True)
        if symbol_ind == 0:
            result = result.reorder_levels(['symbol', 'timestamp'])

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


def plot_weights(d_range, n_plots):
    import matplotlib.pyplot as plt

    w = pd.DataFrame()
    for d in np.linspace(d_range[0], d_range[1], n_plots):
        w_ = get_weights_ffd(d)
        w_ = pd.DataFrame(w_, index=range(w_.shape[0])[::-1], columns=[d])
        w = w.join(w_, how='outer')
    ax = w.plot()
    ax.legend(loc='upper left');
    plt.show()
    return


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
