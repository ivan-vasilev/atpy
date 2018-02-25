"""
Time series utils.
"""

import numpy as np
import pandas as pd

import atpy.data.tradingcalendar as tcal


def set_periods(df: pd.DataFrame):
    """
    Split the dataset into trading/after hours segments using columns (e.g. trading hours or after hours)
    :param df: dataframe (first index have to be datetime)
    :return sliced period
    """

    lc = tcal.open_and_closes.loc[min(df['timestamp']): max(df['timestamp'])]
    lc = pd.concat([lc['market_open'], lc['market_close']]).sort_values()[::-1]

    if not lc.empty:
        df['period'] = np.nan
        df['sequence'] = np.nan
        xs = pd.IndexSlice

        ind = xs[lc.iloc[0]:, :] if isinstance(df.index, pd.MultiIndex) else xs[lc.iloc[0]:]
        df.loc[ind, ['period', 'sequence']] = ('after-hours', 0)
        after_hours_sequence = 1 if df.iloc[-1]['period'] == 'after-hours' else 0

        for i in range(1, len(lc) - 1, 2):
            ind = xs[lc.iloc[i + 1]:lc.iloc[i], :] if isinstance(df.index, pd.MultiIndex) else xs[lc.iloc[i + 1]:lc.iloc[i]]
            df.loc[ind, ['period', 'sequence']] = ('after-hours', after_hours_sequence)
            after_hours_sequence += 1

        ind = xs[:lc.iloc[-1], :] if isinstance(df.index, pd.MultiIndex) else xs[:lc.iloc[-1]]
        df.loc[ind, ['period', 'sequence']] = ('after-hours', after_hours_sequence)

        for i in range(0, len(lc) - 1, 2):
            ind = xs[lc.iloc[i + 1]:lc.iloc[i], :] if isinstance(df.index, pd.MultiIndex) else xs[lc.iloc[i + 1]:lc.iloc[i]]
            df.loc[ind, ['period', 'sequence']] = ('trading-hours', i // 2)


def current_period(df: pd.DataFrame):
    """
    Slice only the current period (e.g. trading hours or after hours)
    :param df: dataframe (first index have to be datettime)
    :return sliced period
    """

    lc = tcal.open_and_closes.loc[min(df['timestamp']): max(df['timestamp'])].iloc[-1]

    xs = pd.IndexSlice
    ind = xs[lc['market_close']:, :] if isinstance(df.index, pd.MultiIndex) else xs[lc['market_close']:]

    result = df.loc[ind]

    if len(result) == 0:
        ind = xs[lc['market_open']:lc['market_close'], :] if isinstance(df.index, pd.MultiIndex) else xs[lc['market_open']:lc['market_close']]
        result = df.loc[ind]

        result['period'] = 'trading-hours'
        result['sequence'] = 0

        return result
    else:
        result['period'] = 'after-hours'
        result['sequence'] = 0

        return result
