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


def adjust_df(data: pd.DataFrame, adjustments: pd.DataFrame):
    """
    IMPORTANT !!! This method supports MultiIndex dataframes
    :param data: dataframe with data.
    :param adjustments: list of adjustments in the form of [(date, split_factor/dividend_amount, 'split'/'dividend'), ...]
    :return adjusted data
    """
    if not data.empty:
        idx = pd.IndexSlice
        start = data.iloc[0].name[0] if isinstance(data.iloc[0].name, tuple) else data.iloc[0].name
        end = data.iloc[-1].name[0] if isinstance(data.iloc[0].name, tuple) else data.iloc[-1].name

        adjustments = adjustments.loc[idx[start:end, list(adjustments.index.levels[1]), :, :], :].sort_index(ascending=False)

        if isinstance(data.index, pd.MultiIndex):
            for (_, row) in adjustments.iterrows():
                if row.name[2] == 'split':
                    adjust_split_multiindex(data=data, symbol=row.name[1], split_date=row.name[0], split_factor=row[0])
                elif row.name[2] == 'dividend':
                    adjust_dividend_multiindex(data=data, symbol=row.name[1], dividend_date=row.name[0], dividend_amount=row[0])
        else:
            for (_, row) in adjustments.iterrows():
                if row.name[2] == 'split':
                    adjust_split(data=data, split_date=row.name[0], split_factor=row[0])
                elif row.name[2] == 'dividend':
                    adjust_dividend(data=data, dividend_date=row.name[0], dividend_amount=row[0])

    return data


def adjust(data, adjustments: pd.DataFrame):
    """
    IMPORTANT !!! This method supports single index df
    :param data: dataframe with data.
    :param adjustments: list of adjustments in the form of [(date, split_factor/dividend_amount, 'split'/'dividend'), ...]
    :return adjusted data
    """
    adjustments.sort(key=lambda x: x[0], reverse=True)

    for (_, row) in adjustments.iterrows():
        if row.name[2] == 'split':
            adjust_split(data=data, split_date=row.name[0], split_factor=row[0])
        elif row.name[2] == 'dividend':
            adjust_dividend(data=data, dividend_date=row.name[0], dividend_amount=row[0])

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


def adjust_split(data, split_factor: float, split_date: datetime.date, idx: pd.IndexSlice=None):
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


def adjust_dividend_multiindex(data: pd.DataFrame, symbol: str, dividend_amount: float, dividend_date: datetime.date):
    if len(data) > 0 and symbol in data.index.levels[1]:
        dividend_date = datetime.datetime.combine(dividend_date, datetime.datetime.min.time()).replace(tzinfo=data.iloc[0].name[0].tz)

        idx = pd.IndexSlice

        if data.loc[idx[:, symbol], :].iloc[0].name[0] <= dividend_date <= data.loc[idx[:, symbol], :].iloc[-1].name[0]:
            for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                data.loc[idx[:dividend_date, symbol], c] -= dividend_amount


def adjust_split_multiindex(data: pd.DataFrame, symbol: str, split_factor: float, split_date: datetime.date):
    if split_factor > 0 and len(data) > 0 and symbol in data.index.levels[1]:
        split_date = datetime.datetime.combine(split_date, datetime.datetime.min.time()).replace(tzinfo=data.iloc[0].name[0].tz)

        idx = pd.IndexSlice

        if data.loc[idx[:, symbol], :].iloc[0].name[0] <= split_date <= data.loc[idx[:, symbol], :].iloc[-1].name[0]:
            for c in [c for c in ['period_volume', 'total_volume', 'last_size'] if c in data.columns]:
                data.loc[idx[:split_date, symbol], c] *= (1 / split_factor)
                data.loc[idx[:split_date, symbol], c] = data[c].astype(np.uint64)
            for c in [c for c in ['close', 'high', 'open', 'low', 'ask', 'bid', 'last'] if c in data.columns]:
                data.loc[idx[:split_date, symbol], c] *= split_factor
