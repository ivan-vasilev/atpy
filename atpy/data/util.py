import pandas as pd
from ftplib import FTP
from io import StringIO


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
    return pd.read_csv(StringIO(r.data), sep="|")


def get_nasdaq_listed_companies():
    return _get_nasdaq_symbol_file('nasdaqlisted.txt')


def get_non_nasdaq_listed_companies():
    return _get_nasdaq_symbol_file('otherlisted.txt')
