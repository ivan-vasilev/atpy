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


__open_and_closes_series = pd.concat([tcal.open_and_closes['market_open'], tcal.open_and_closes['market_close']]).sort_values()
__closes_series = tcal.open_and_closes['market_close']


def current_period(df: pd.DataFrame):
    """
    Slice only the current period (e.g. trading hours or after hours)
    :param df: dataframe (first index have to be datettime)
    :return sliced period
    """

    most_recent = df.iloc[-1].timestamp

    xs = pd.IndexSlice

    if most_recent.date() in tcal.trading_days:
        current_hours = __open_and_closes_series.loc[most_recent.date()]
        if most_recent > current_hours[1]:
            result, period = df.loc[xs[current_hours[1]:, :] if isinstance(df.index, pd.MultiIndex) else xs[current_hours[1]:]], 'after-hours'
        else:
            result, period = df.loc[xs[current_hours[0]:, :] if isinstance(df.index, pd.MultiIndex) else xs[current_hours[0]:]], 'trading-hours'
    else:
        lc = __closes_series.loc[df.iloc[0].timestamp.date(): most_recent.date()][::-1]

        for i in range(len(lc)):
            result = df.loc[xs[lc.iloc[i]:, :] if isinstance(df.index, pd.MultiIndex) else xs[lc.iloc[i]:]]
            if not result.empty:
                period = 'after-hours'
                break
        else:
            result, period = df, 'after-hours'

    return result, period
