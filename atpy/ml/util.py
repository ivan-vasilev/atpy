import functools
import typing
from multiprocessing import Pool, cpu_count

import pandas as pd
from pandas.core.groupby import GroupBy


def pd_apply_parallel(df_groupby: GroupBy, func: typing.Callable, *func_args):
    """
    Pandas parallel apply
    :param df_groupby: GroupBy - the result from DataFrame.groupby() call)
    :param func: apply function
    :param func_args: list of arguments to be passed to the func call
    :return processed dataframe (or series)
    """
    with Pool(cpu_count()) as p:
        if len(func_args) == 0:
            ret_list = p.map(func, [group for name, group in df_groupby])
        else:
            ret_list = p.starmap(func, [(group,) + func_args for name, group in df_groupby])

    return pd.concat(ret_list)


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
