import functools
import typing
from multiprocessing import Pool, cpu_count

import pandas as pd
from pandas.core.groupby import GroupBy


def pd_apply_parallel(df_groupby: GroupBy, func: typing.Callable):
    """
    Pandas parallel apply
    :param df_groupby: GroupBy - the result from DataFrame.groupby() call)
    :param func: apply function
    :return processed dataframe (or series)
    """
    with Pool(cpu_count()) as p:
        ret_list = p.map(func, [group for name, group in df_groupby])

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

    return pd.Series(index=pd.MultiIndex.from_tuples(result, names=df.index.names))


def cumsum_filter(df, parallel=True):
    """
    cumsum filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param df: multi-index pd.DataFrame with 2 columns - first for the value and the second for threshold
    :param parallel: run in multiprocessing mode for multiindex dataframes
    :return events
    """
    if isinstance(df.index, pd.MultiIndex):
        grpby = df.groupby(level='symbol', group_keys=False)
        result = pd_apply_parallel(grpby, _cumsum_filter) if parallel else grpby.apply(_cumsum_filter)
    else:
        result = _cumsum_filter(df)

    return result.index


def _daily_volatility(price, span=100):
    """
    Compute daily volatility  filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param price: single index pd.Series
    :param span: number of days to include in the volatility computation
    :return events
    """

    if isinstance(price.index, pd.MultiIndex):
        symbol_ind = price.index.names.index('symbol')
        symbol = price.index[0][symbol_ind]
        p = price.loc[:, symbol] if symbol_ind == 1 else price.loc[symbol]
    else:
        symbol_ind, symbol = -1, None

    tmp = p.index.searchsorted(p.index - pd.Timedelta(days=1))
    tmp = tmp[tmp > 0]
    tmp = p.index[tmp - 1].to_series(keep_tz=True, index=p.index[p.shape[0] - tmp.shape[0]:])
    tmp = p.loc[tmp.index] / p.loc[tmp.values].values - 1
    tmp = tmp.ewm(span=span).std().dropna()

    if symbol_ind > -1:
        tmp = tmp.to_frame()
        tmp['symbol'] = symbol
        tmp.set_index('symbol', append=True, inplace=True, drop=True)
        if symbol_ind == 0:
            tmp.reorder_levels(['symbol', 'timestamp'])

        tmp = tmp[tmp.columns[0]]

    return tmp


def daily_volatility(price, span=100, parallel=True):
    """
    Compute daily volatility  filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param price: single index pd.Series
    :param span: number of days to include in the volatility computation
    :param parallel: run in multiprocessing mode for multiindex dataframes
    :return events
    """
    if isinstance(price.index, pd.MultiIndex):
        grpby = price.groupby(level='symbol', group_keys=False, sort=False)
        f = functools.partial(_daily_volatility, span=span)
        result = pd_apply_parallel(grpby, f) if parallel else grpby.apply(f)
    else:
        result = _daily_volatility(price)

    return result
