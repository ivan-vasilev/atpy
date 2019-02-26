"""
Time series utils.
"""
import datetime
import queue
import threading
import typing

import pandas as pd
from dateutil.relativedelta import relativedelta

import atpy.data.tradingcalendar as tcal


def set_periods(df: pd.DataFrame):
    """
    Split the dataset into trading/after hours segments using columns (e.g. trading hours or after hours)
    :param df: dataframe (first index have to be datetime)
    :return sliced period
    """

    df['period'] = 'after-hours'

    lc = tcal.open_and_closes.loc[df.iloc[0].name[0].date():df.iloc[-1].name[0].date()]

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

    most_recent = df.iloc[-1].name[0]

    try:
        current_hours = __open_and_closes_series.loc[most_recent.date()]
    except KeyError:
        current_hours = None

    if current_hours is not None:
        if most_recent > current_hours[1]:
            result, period = df.loc[current_hours[1]:], 'after-hours'
        elif most_recent < current_hours[0]:
            lc = __closes_series.loc[df.iloc[0].name[0].date(): most_recent.date()]
            if len(lc) > 1:
                result, period = df.loc[lc[-2]:], 'after-hours'
            else:
                result, period = df, 'after-hours'
        else:
            result, period = df.loc[current_hours[0]:], 'trading-hours'
    else:
        lc = __closes_series.loc[df.iloc[0].name[0].date(): most_recent.date()]
        if len(lc) > 0:
            result, period = df.loc[lc.iloc[-1]:], 'after-hours'
        else:
            result, period = df, 'after-hours'

    return result, period


def current_phase(dttme):
    """
    Get current phase (trading/after-hours)
    :param dttme: datetime
    :return phase
    """

    if dttme.date() in tcal.trading_days:
        current_hours = tcal.open_and_closes.loc[dttme.date()]
        return 'trading-hours' if current_hours['market_open'] <= dttme <= current_hours['market_close'] else 'after-hours'
    else:
        return 'after-hours'


def current_day(df: pd.DataFrame, tz=None):
    """
    Slice only the current day data
    :param df: dataframe (first index have to be datettime)
    :param tz: timezone
    :return sliced period
    """
    d = df.iloc[-1].name[0].normalize()
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


def gaps(df: pd.DataFrame):
    """
    Compute percent changes in the price
    :param df: pandas OHLC DataFrame
    :return DataFrame with changes
    """

    result = df.groupby('symbol', level='symbol').agg({'low': 'min', 'high': 'max'})

    low = result['low']
    result = (result['high'] - low) / low

    return result


def rolling_mean(df: pd.DataFrame, window: int, column: typing.Union[typing.List, str] = 'close'):
    """
    Compute the rolling mean over a column
    :param df: pandas OHLC DataFrame
    :param window: window size OHLC DataFrame
    :param column: a column (or list of columns, where to apply the rolling mean)
    :return DataFrame with changes
    """
    return df[column].groupby(level='symbol', group_keys=False).rolling(window).mean()


def ohlc_mean(df: pd.DataFrame):
    """
    Compute the mean value of o/h/l/c
    :param df: pandas OHLC DataFrame
    :return DataFrame with changes
    """
    df[['open', 'open', 'high', 'low']].mean(axis=1)


def overlap_by_symbol(old_df: pd.DataFrame, new_df: pd.DataFrame, overlap: int):
    """
    Overlap dataframes for timestamp continuity. Prepend the end of old_df to the beginning of new_df, grouped by symbol.
    If no symbol exists, just overlap the dataframes
    :param old_df: old dataframe
    :param new_df: new dataframe
    :param overlap: number of time steps to overlap
    :return DataFrame with changes
    """
    if isinstance(old_df.index, pd.MultiIndex) and isinstance(new_df.index, pd.MultiIndex):
        old_df_tail = old_df.groupby(level='symbol').tail(overlap)

        old_df_tail = old_df_tail.drop(set(old_df_tail.index.get_level_values('symbol')) - set(new_df.index.get_level_values('symbol')), level='symbol')

        return pd.concat([old_df_tail, new_df], sort=True)
    else:
        return pd.concat([old_df.tail(overlap), new_df], sort=True)


class AsyncInPeriodProvider(object):
    """
    Run InPeriodProvider in async mode
    """

    def __init__(self, in_period_provider: typing.Iterable):
        """
        :param in_period_provider: provider
        """

        self.in_period_provider = in_period_provider

    def __iter__(self):
        self._q = queue.Queue()

        self.in_period_provider.__iter__()

        def it():
            for i in self.in_period_provider:
                self._q.put(i)

            self._q.put(None)

        threading.Thread(target=it, daemon=True).start()

        return self

    def __next__(self):
        result = self._q.get()
        if result is None:
            raise StopIteration()

        return result
