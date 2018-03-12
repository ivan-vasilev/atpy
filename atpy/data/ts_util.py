"""
Time series utils.
"""
import datetime

import pandas as pd
from dateutil import relativedelta

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
        elif most_recent < current_hours[0]:
            lc = __closes_series.loc[df.iloc[0].timestamp.date(): most_recent.date()]
            if len(lc) > 1:
                result, period = df.loc[xs[lc[-2]:, :] if isinstance(df.index, pd.MultiIndex) else xs[lc[-2]:]], 'after-hours'
            else:
                result, period = df, 'after-hours'
        else:
            result, period = df.loc[xs[current_hours[0]:, :] if isinstance(df.index, pd.MultiIndex) else xs[current_hours[0]:]], 'trading-hours'
    else:
        lc = __closes_series.loc[df.iloc[0].timestamp.date(): most_recent.date()]
        if len(lc) > 0:
            result, period = df.loc[xs[lc.iloc[-1]:, :] if isinstance(df.index, pd.MultiIndex) else xs[lc.iloc[-1]:]], 'after-hours'
        else:
            result, period = df, 'after-hours'

    return result, period


def current_day(df: pd.DataFrame, tz=None):
    """
    Slice only the current day data
    :param df: dataframe (first index have to be datettime)
    :param tz: timezone
    :return sliced period
    """
    d = df.iloc[-1].timestamp.normalize()
    if tz is not None:
        d = d.tz_convert(tz).tz_localize(None).tz_localize(d.tzinfo)

    xs = pd.IndexSlice

    return df.loc[xs[d:, :] if isinstance(df.index, pd.MultiIndex) else xs[d:]]


def slice_periods(bgn_prd: datetime.datetime, delta: relativedelta, ascend: bool = True, overlap: relativedelta = None):
    """
    Split time interval in delta-sized intervals
    :param bgn_prd: begin period
    :param delta: delta
    :param ascend: ascending/descending
    :param overlap: whether to provide overlap within the intervals
    :return sliced period
    """

    overlap = overlap if overlap is not None else relativedelta(days=0)

    result = list()
    if ascend:
        now = datetime.datetime.now(tz=bgn_prd.tzinfo)

        while bgn_prd < now:
            end_prd = min(bgn_prd + delta + overlap, now)
            result.append((bgn_prd, end_prd))
            bgn_prd = bgn_prd + delta
    else:
        end_prd = datetime.datetime.now(tz=bgn_prd.tzinfo)

        while end_prd > bgn_prd:
            result.append((max(end_prd - delta - overlap, bgn_prd), end_prd))
            end_prd = end_prd - delta

    return result
