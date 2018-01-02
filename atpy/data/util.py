import datetime
from ftplib import FTP
from io import StringIO

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


def adjust(symbol: str, data: pd.DataFrame, splits: list=None, dividends: list=None):
    """
    :param symbol: string
    :param data: dataframe with data
    :param splits: [(split_date, split_ratio), ...]
    :param dividends: [(dividend_date, dividend_amount), ...]
    :return None, the data is adjusted inplace
    """
    by_date = list()
    if splits:
        by_date += [(s[0], s[1], 'split') for s in splits if s is not None and None not in s]

    if dividends:
        by_date += [(d[0], d[1], 'dividend') for d in dividends if d is not None and None not in d]

    by_date.sort(key=lambda x: x[0], reverse=True)

    for e in by_date:
        if e[2] == 'split':
            adjust_split(data=data, symbol=symbol, split_date=e[0], split_factor=e[1])
        elif e[2] == 'dividend':
            adjust_dividend(data=data, symbol=symbol, dividend_date=e[0], dividend_amount=e[1])


def adjust_dividend(data, symbol, dividend_amount, dividend_date):
    if isinstance(data, pd.DataFrame):
        if 'open' in data.columns:  # adjust bars
            if isinstance(data.index, pd.MultiIndex):
                dividend_date = datetime.datetime.combine(dividend_date.astype(datetime.datetime), datetime.datetime.min.time()).replace(tzinfo=data.index.levels[1].tz)
                mask = data['timestamp'] < dividend_date
                mask[data['symbol'] != symbol] = False

                data.loc[mask, 'close'] -= dividend_amount
                data.loc[mask, 'high'] -= dividend_amount
                data.loc[mask, 'open'] -= dividend_amount
                data.loc[mask, 'low'] -= dividend_amount
            else:
                data.loc[data.index < dividend_date, 'close'] -= dividend_amount
                data.loc[data.index < dividend_date, 'high'] -= dividend_amount
                data.loc[data.index < dividend_date, 'open'] -= dividend_amount
                data.loc[data.index < dividend_date, 'low'] -= dividend_amount
        elif 'ask' in data.columns:  # adjust ticks:
            data.loc[data['timestamp'] < dividend_date, 'ask'] -= dividend_amount
            data.loc[data['timestamp'] < dividend_date, 'bid'] -= dividend_amount
            data.loc[data['timestamp'] < dividend_date, 'last'] -= dividend_amount
    elif dividend_date > data['timestamp']:
        if 'open' in data:  # adjust bars
            data['open'] -= dividend_amount
            data['close'] -= dividend_amount
            data['high'] -= dividend_amount
            data['low'] -= dividend_amount
        elif 'ask' in data:  # adjust ticks:
            data['ask'] -= dividend_amount
            data['bid'] -= dividend_amount
            data['last'] -= dividend_amount


def adjust_split(data, symbol, split_factor, split_date):
    if isinstance(data, pd.DataFrame):
        if 'open' in data.columns:  # adjust bars
            if isinstance(data.index, pd.MultiIndex):
                split_date = datetime.datetime.combine(split_date.astype(datetime.datetime), datetime.datetime.min.time()).replace(tzinfo=data.index.levels[1].tz)
                mask = data['timestamp'] < split_date
                mask[data['symbol'] != symbol] = False

                data.loc[mask, 'open'] *= split_factor
                data.loc[mask, 'close'] *= split_factor
                data.loc[mask, 'high'] *= split_factor
                data.loc[mask, 'low'] *= split_factor
                data.loc[mask, 'period_volume'] *= int(1 / split_factor)
                data.loc[mask, 'total_volume'] *= int(1 / split_factor)
            else:
                data.loc[data.index < split_date, 'open'] *= split_factor
                data.loc[data.index < split_date, 'close'] *= split_factor
                data.loc[data.index < split_date, 'high'] *= split_factor
                data.loc[data.index < split_date, 'low'] *= split_factor
                data.loc[data.index < split_date, 'period_volume'] *= int(1 / split_factor)
                data.loc[data.index < split_date, 'total_volume'] *= int(1 / split_factor)
        elif 'ask' in data.columns:  # adjust ticks:
            data.loc[data['timestamp'] < split_date, 'ask'] *= split_factor
            data.loc[data['timestamp'] < split_date, 'bid'] *= split_factor
            data.loc[data['timestamp'] < split_date, 'last'] *= split_factor
            data.loc[data['timestamp'] < split_date, 'last_size'] *= int(1 / split_factor)
            data.loc[data['timestamp'] < split_date, 'total_volume'] *= int(1 / split_factor)
    elif split_factor > 0 and split_date > data['timestamp']:
        if 'open' in data:  # adjust bars
            data['open'] *= split_factor
            data['close'] *= split_factor
            data['high'] *= split_factor
            data['low'] *= split_factor
            data['period_volume'] *= int(1 / split_factor)
            data['total_volume'] *= int(1 / split_factor)
        elif 'ask' in data:  # adjust ticks:
            data['ask'] *= split_factor
            data['bid'] *= split_factor
            data['last'] *= split_factor
            data['last_size'] *= int(1 / split_factor)
            data['total_volume'] *= int(1 / split_factor)
