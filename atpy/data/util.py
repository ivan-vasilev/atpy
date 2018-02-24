import datetime
from ftplib import FTP
from io import StringIO

import numpy as np
import pandas as pd

import atpy.data.tradingcalendar as tcal


def _get_nasdaq_symbol_file(filename):
    ftp = FTP('ftp.nasdaqtrader.com')
    ftp.login()
    ftp.cwd('symboldirectory')

    class Reader:
        def __init__(self):
            self.data = ""

        def __call__(self, s):
            self.data += s.decode('ascii')

    r = Reader()

    ftp.retrbinary('RETR ' + filename, r)
    return pd.read_csv(StringIO(r.data), sep="|")[:-1]


def get_nasdaq_listed_companies():
    result = _get_nasdaq_symbol_file('nasdaqlisted.txt')
    result = result[result['Financial Status'] == 'N']
    result = result[result['Test Issue'] == 'N']

    include_only = set()
    include_only_index = list()
    for i in range(result.shape[0]):
        s = result.iloc[i]
        if len(s['symbol']) < 5 or s['symbol'][:4] not in include_only:
            include_only_index.append(True)
            include_only.add(s['symbol'])
        else:
            include_only_index.append(False)

    result = result[include_only_index]
    result = list(result['symbol'].str.replace('$', '-'))

    for i, s in enumerate(result):
        try:
            pos = s.index('.')
        except:
            pass
        else:
            s = s[:pos]

        result[i] = s

    result.sort()

    return pd.DataFrame(result)


def get_non_nasdaq_listed_companies():
    result = _get_nasdaq_symbol_file('otherlisted.txt')
    result = result[result['Test Issue'] == 'N']

    result = list(result['ACT Symbol'].str.replace('$', '-'))

    for i, s in enumerate(result):
        try:
            pos = s.index('.')
        except:
            pass
        else:
            s = s[:pos]

        result[i] = s

    result.sort()

    return pd.DataFrame(result)


def get_us_listed_companies():
    nd = get_nasdaq_listed_companies()
    non_nd = get_non_nasdaq_listed_companies()
    symbols = list(set(list(non_nd[0]) + list(nd[0])))
    symbols.sort()

    return pd.DataFrame(symbols)


def adjust(data, adjustments: list):
    """
    IMPORTANT !!! This method supports MultiIndex dataframes, but the first level of the index has to be of ONLY ONE symbol
    :param data: dataframe with data.
    :param adjustments: list of adjustments in the form of [(date, split_factor/dividend_amount, 'split'/'dividend'), ...]
    :return adjusted data
    """
    adjustments.sort(key=lambda x: x[0], reverse=True)

    for e in adjustments:
        if e[2] == 'split':
            adjust_split(data=data, split_date=e[0], split_factor=e[1])
        elif e[2] == 'dividend':
            adjust_dividend(data=data, dividend_date=e[0], dividend_amount=e[1])

    return data


def adjust_dividend(data, dividend_amount: float, dividend_date: datetime.date):
    if isinstance(data, pd.DataFrame):
        if len(data) > 0:
            dividend_date = datetime.datetime.combine(dividend_date, datetime.datetime.min.time()).replace(tzinfo=data.iloc[0]['timestamp'].tz)

            if dividend_date > data.iloc[0]['timestamp']:
                for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                    data[c] -= dividend_amount
            elif dividend_date > data.iloc[-1]['timestamp']:
                for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                    data.loc[data['timestamp'] < dividend_date, c] -= dividend_amount
    elif dividend_date > data['timestamp']:
        for c in [c for c in {'close', 'high', 'open', 'low', 'ask', 'bid', 'last'} if c in data.keys()]:
            data[c] -= dividend_amount


def adjust_split(data, split_factor: float, split_date: datetime.date):
    if split_factor > 0:
        if isinstance(data, pd.DataFrame):
            if len(data) > 0:
                split_date = datetime.datetime.combine(split_date, datetime.datetime.min.time()).replace(tzinfo=data.iloc[0]['timestamp'].tz)

                if split_date > data.iloc[-1]['timestamp']:
                    for c in [c for c in ['period_volume', 'total_volume', 'last_size'] if c in data.columns]:
                        data[c] = (data[c] * (1 / split_factor)).astype(np.uint64)

                    for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                        data[c] *= split_factor
                elif split_date > data.iloc[0]['timestamp']:
                    for c in [c for c in ['period_volume', 'total_volume', 'last_size'] if c in data.columns]:
                        data.loc[data['timestamp'] < split_date, c] *= (1 / split_factor)
                        data[c] = data[c].astype(np.uint64)

                    for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                        data.loc[data['timestamp'] < split_date, c] *= split_factor
        elif split_date > data['timestamp']:
            for c in [c for c in {'close', 'high', 'open', 'low', 'period_volume', 'total_volume', 'ask', 'bid', 'last', 'last_size'} if c in data.keys()]:
                if c in ('period_volume', 'total_volume', 'last_size'):
                    data[c] = int(data[c] * (1 / split_factor))
                else:
                    data[c] *= split_factor


def current_period(df: pd.DataFrame):
    """
    Slice only the current period (e.g. trading hours or after hours)
    :param df: dataframe (first index have to be datettime)
    :return sliced period
    """

    lc = tcal.open_and_closes.loc[min(df['timestamp']): max(df['timestamp'])].iloc[-1]
    result = df.loc[lc['market_close']:]
    if len(result) == 0:
        result = df.loc[lc['market_open']:lc['market_close']]
        return result, 'trading-hours'
    else:
        return result, 'after-hours'
