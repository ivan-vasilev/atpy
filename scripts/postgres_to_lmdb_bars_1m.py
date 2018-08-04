#!/bin/python3

import argparse
import datetime
import functools
import logging
import os

import psycopg2
from dateutil.relativedelta import relativedelta

from atpy.data.cache.lmdb_cache import *
from atpy.data.cache.postgres_cache import BarsInPeriodProvider, request_adjustments
from atpy.data.splits_dividends import adjust_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="PostgreSQL to LMDB configuration")
    parser.add_argument('-lmdb_path', type=str, default=None, help="LMDB Path")
    parser.add_argument('-delta_back', type=int, default=8, help="Default number of years to look back")
    parser.add_argument('-adjust_splits', action='store_true', default=True, help="Adjust splits before saving")
    parser.add_argument('-adjust_dividends', action='store_true', default=False, help="Adjust dividends before saving")

    args = parser.parse_args()

    lmdb_path = args.lmdb_path if args.lmdb_path is not None else os.environ['ATPY_LMDB_PATH']

    con = psycopg2.connect(os.environ['POSTGRESQL_CACHE'])

    adjustments = None
    if args.adjust_splits and args.adjust_dividends:
        adjustments = request_adjustments(conn=con, table_name='splits_dividends')
    elif args.adjust_splits:
        adjustments = request_adjustments(conn=con, table_name='splits_dividends', adj_type='split')
    elif args.adjust_dividends:
        adjustments = request_adjustments(conn=con, table_name='splits_dividends', adj_type='dividend')

    now = datetime.datetime.now()
    bgn_prd = datetime.datetime(now.year - args.delta_back, 1, 1)
    bgn_prd = bgn_prd + relativedelta(days=7 - bgn_prd.weekday())

    cache_read = functools.partial(read_pickle, lmdb_path=lmdb_path)
    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=60, interval_type='s', bars_table='bars_1m', bgn_prd=bgn_prd, delta=relativedelta(days=7),
                                          overlap=relativedelta(microseconds=-1), cache=cache_read)

    for i, df in enumerate(bars_in_period):
        if cache_read(bars_in_period.current_cache_key()) is None:
            if adjustments is not None:
                adjust_df(df, adjustments)

            write(bars_in_period.current_cache_key(), df, lmdb_path)
            logging.info('Saving ' + bars_in_period.current_cache_key())
        else:
            logging.info('Cache hit on ' + bars_in_period.current_cache_key())
