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
        if len(s['Symbol']) < 5 or s['Symbol'][:4] not in include_only:
            include_only_index.append(True)
            include_only.add(s['Symbol'])
        else:
            include_only_index.append(False)

    result = result[include_only_index]
    result = list(result['Symbol'].str.replace('$', '-'))

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
