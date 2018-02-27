"""
Time series utils.
"""

import pandas as pd

import atpy.data.tradingcalendar as tcal


def set_periods(df: pd.DataFrame):
    """
    Split the dataset into trading/after hours segments using columns (e.g. trading hours or after hours)
    :param df: dataframe (first index have to be datetime)
    :return sliced period
    """

    df['period'] = 'after-hours'

    lc = tcal.open_and_closes.loc[df.iloc[0].timestamp.date():df.iloc[-1].timestamp.date()]

    xs = pd.IndexSlice

    def a(x):
        df.loc[xs[x['market_open']:x['market_close'], :] if isinstance(df.index, pd.MultiIndex) else xs[x['market_open']:x['market_close']], 'period'] = 'trading-hours'

    lc.apply(a, axis=1)


def current_period(df: pd.DataFrame):
    """
    Slice only the current period (e.g. trading hours or after hours)
    :param df: dataframe (first index have to be datettime)
    :return sliced period
    """

    lc = tcal.open_and_closes.loc[df.iloc[0].timestamp.date():df.iloc[-1].timestamp.date()]
    lc = pd.concat([lc['market_open'], lc['market_close']]).sort_values()[::-1]

    xs = pd.IndexSlice

    for i in range(len(lc) + 1):
        if i == 0:
            result, period = df.loc[xs[lc.iloc[i]:, :] if isinstance(df.index, pd.MultiIndex) else xs[lc.iloc[i]:]], 'after-hours'
        elif i == len(lc):
            result, period = df.loc[xs[:lc.iloc[-1], :] if isinstance(df.index, pd.MultiIndex) else xs[:lc.iloc[-1]]], 'after-hours'
        elif i % 2 == 0:
            result, period = df.loc[xs[lc.iloc[i]:lc.iloc[i - 1], :] if isinstance(df.index, pd.MultiIndex) else xs[lc.iloc[i]:lc.iloc[i - 1]]], 'after-hours'
        elif i % 2 == 1:
            result, period = df.loc[xs[lc.iloc[i]:lc.iloc[i - 1], :] if isinstance(df.index, pd.MultiIndex) else xs[lc.iloc[i]:lc.iloc[i - 1]]], 'trading-hours'

        if not result.empty:
            return result, period
