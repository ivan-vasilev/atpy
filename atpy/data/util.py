import datetime
from ftplib import FTP
from io import StringIO

import numpy as np
import pandas as pd


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


def adjust(symbol: str, data, adjustments: list):
    """
    :param symbol: string
    :param data: dataframe with data
    :param adjustments: list of adjustments in the form of [(date, split_factor/dividend_amount, 'split'/'dividend'), ...]
    :return None, the data is adjusted inplace
    """
    adjustments.sort(key=lambda x: x[0], reverse=True)

    for e in adjustments:
        if e[2] == 'split':
            adjust_split(data=data, split_date=e[0], split_factor=e[1])
        elif e[2] == 'dividend':
            adjust_dividend(data=data, dividend_date=e[0], dividend_amount=e[1])


def adjust_dividend(data, dividend_amount: float, dividend_date: datetime.date):
    if isinstance(data, pd.DataFrame):
        cols = [c for c in {'close', 'high', 'open', 'low', 'ask', 'bid', 'last'} if c in data.columns]
        if isinstance(data.index, pd.MultiIndex):
            dividend_date = datetime.datetime.combine(dividend_date, datetime.datetime.min.time()).replace(tzinfo=data.index.levels[1].tz)

            for c in cols:
                data.loc[data['timestamp'] < dividend_date, c] -= dividend_amount
        else:
            dividend_date = datetime.datetime.combine(dividend_date, datetime.datetime.min.time()).replace(tzinfo=data.index.tz)
            for c in [c for c in {'close', 'high', 'open', 'low', 'ask', 'bid', 'last'} if c in data.columns]:
                data.loc[data.index < dividend_date, c] -= dividend_amount
    elif dividend_date > data['timestamp']:
        for c in [c for c in {'close', 'high', 'open', 'low', 'ask', 'bid', 'last'} if c in data.keys()]:
            data[c] -= dividend_amount


def adjust_split(data, split_factor: float, split_date: datetime.date):
    if split_factor > 0:
        if isinstance(data, pd.DataFrame):
            cols = [c for c in {'close', 'high', 'open', 'low', 'period_volume', 'total_volume', 'ask', 'bid', 'last', 'last_size'} if c in data.columns]

            if isinstance(data.index, pd.MultiIndex):
                split_date = datetime.datetime.combine(split_date, datetime.datetime.min.time()).replace(tzinfo=data.index.levels[1].tz)

                for c in cols:
                    if c in ('period_volume', 'total_volume', 'last_size'):
                        col = data.loc[data.index.levels[1] < split_date]
                        col *= (1 / split_factor)
                        data.loc[data.index.levels[1] < split_date] = col.astype(np.uint64)
                    else:
                        data.loc[data.index.levels[1] < split_date, c] *= split_factor
            else:
                split_date = datetime.datetime.combine(split_date, datetime.datetime.min.time()).replace(tzinfo=data.index.tz)
                for c in cols:
                    if c in ('period_volume', 'total_volume', 'last_size'):
                        data.loc[data.index < split_date, c] = (data.loc[data.index < split_date, c] * (1 / split_factor)).astype(np.uint64)
                    else:
                        data.loc[data.index < split_date, c] *= split_factor
        elif split_date > data['timestamp']:
            for c in [c for c in {'close', 'high', 'open', 'low', 'period_volume', 'total_volume', 'ask', 'bid', 'last', 'last_size'} if c in data.keys()]:
                if c in ('period_volume', 'total_volume', 'last_size'):
                    data[c] = int(data[c] * (1 / split_factor))
                else:
                    data[c] *= split_factor
