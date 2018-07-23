import functools
import typing
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd
from numba import jit


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


def _triple_barriers(data: pd.Series, pt: pd.Series, sl: pd.Series, vb: pd.Timedelta):
    """
    Triple barrier labeling for single index series
    :param data: data to be labeled
    :param vb: vertical barrier delta
    :param pt: profit taking barrier
    :param sl: stop loss barrier
    :return dataframe with first threshold crossings
    """
    intervals = data.rolling(vb, closed="both").count().astype(np.int)
    result = data.to_frame()
    result['pt'] = data.index.copy()
    result['sl'] = data.index.copy()
    result['vb'] = data.index.copy()
    result.drop(data.name, axis=1, inplace=True)

    __pt_sl(data=data.values, timestamps=data.index.values, pt=pt.values, sl=sl.values, intervals=intervals.values, pt_result=result['pt'].values, sl_result=result['sl'].values, vb_result=result['vb'].values)
    result.loc[result['pt'] == data.index, 'pt'] = pd.NaT
    result.loc[result['sl'] == data.index, 'sl'] = pd.NaT
    result.loc[result['vb'] == data.index, 'vb'] = pd.NaT

    return result


#@jit(nopython=True)
def __pt_sl(data: np.array, timestamps: np.array, pt: np.array, sl: np.array, intervals: np.array, pt_result: np.array, sl_result=np.array, vb_result=np.array):
    """This function cannot be local to _triple_barriers, because it will be compiled on every groupby"""

    for i in range(data.size, 0, -1):
        length = intervals[i - 1]
        current = i - length
        if current > 0:
            vb_result[current] = timestamps[i - 1]

            interval = data[current: i]
            returns = interval / interval[0] - 1

            # profit take
            delta = pt[current]
            for j in range(1, length):
                if returns[j] > delta:
                    pt_result[current] = timestamps[current + j]
                    break

            # stop loss
            delta = -sl[current]
            for j in range(1, length):
                if returns[j] < delta:
                    sl_result[current] = timestamps[current + j]
                    break
        else:
            return


def triple_barriers(data: pd.Series, pt: pd.Series, sl: pd.Series, vb: pd.Timedelta, parallel=True):
    """
    Triple barrier labeling for single multiindex series
    :param data: data to be labeled
    :param vb: vertical barrier delta
    :param pt: profit taking barrier
    :param sl: stop loss barrier
    :param parallel: run in parallel
    :return dataframe with first threshold crossings
    """
    if isinstance(data.index, pd.DatetimeIndex):
        return _triple_barriers(data=data, pt=pt, sl=sl, vb=vb)
    else:
        data = data.astype(np.float32).to_frame().rename(columns={data.name: 'data'}) if data.dtype != np.float32 else data.to_frame().rename(columns={data.name: 'data'})

        if pt is not None:
            data['pt'] = pt.astype(np.float32) if pt.dtype != np.float32 else pt
        if sl is not None:
            data['sl'] = sl.astype(np.float32) if sl.dtype != np.float32 else sl

        if parallel:
            with Pool(cpu_count()) as p:
                ret_list = p.starmap(__triple_barriers_groupby, [(group, vb) for name, group in data.groupby(level='symbol', group_keys=False, sort=False)])
                return pd.concat(ret_list)
        else:
            return data.groupby(level='symbol', group_keys=False, sort=False).apply(__triple_barriers_groupby, vb=vb)


def __triple_barriers_groupby(data: pd.DataFrame, vb: pd.Timedelta):
    """
    Triple barrier labeling for single index series, specific for groupby
    :param data: dataframe with 3 columns - the data itself, pt (profit take) and sl (stop loss) to be labeled
    :param vb: vertical barrier delta
    :return dataframe with first threshold crossings
    """
    if isinstance(data.index, pd.MultiIndex):
        symbol_ind = data.index.names.index('symbol')
        symbol = data.index[0][symbol_ind]
        data = data.loc[pd.IndexSlice[:, symbol]] if symbol_ind == 1 else data.loc[symbol]
    else:
        symbol_ind, symbol = -1, None

    result = _triple_barriers(data['data'], pt=data['pt'] if 'pt' in data else None, sl=data['sl'] if 'sl' in data else None, vb=vb)

    if symbol_ind > -1:
        result['symbol'] = symbol
        result.set_index('symbol', append=True, inplace=True, drop=True)
        if symbol_ind == 0:
            result = result.reorder_levels(['symbol', 'timestamp'])

    return result
