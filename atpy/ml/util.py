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


def cumsum_filter(df: pd.DataFrame):
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


def multiindex_cusum_filter(df, parallel=True):
    """
    cumsum filter based on Advances in Financial Machine Learning book by Marcos Lopez de Prado
    :param df: multi-index pd.DataFrame with 2 columns - first for the value and the second for threshold
    :return events
    """
    if isinstance(df.index, pd.MultiIndex):
        grpby = df.groupby(level='symbol', group_keys=False)
        result = pd_apply_parallel(grpby, cumsum_filter) if parallel else grpby.apply(cumsum_filter)
    else:
        result = cumsum_filter(df)

    return result.index
