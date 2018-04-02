import argparse
import datetime
import logging
import os

import psycopg2
from dateutil.relativedelta import relativedelta

from atpy.data.cache.lmdb_cache import *
from atpy.data.cache.postgres_cache import BarsInPeriodProvider

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="PostgreSQL to LMDB configuration")
    parser.add_argument('-lmdb_path', type=str, default=None, help="LMDB Path")
    parser.add_argument('-delta_back', type=int, default=8, help="Default number of years to look back")
    args = parser.parse_args()

    lmdb_path = args.lmdb_path if args.lmdb_path is not None else os.environ['ATPY_LMDB_PATH']

    con = psycopg2.connect(os.environ['POSTGRESQL_CACHE'])

    now = datetime.datetime.now()
    bgn_prd = datetime.datetime(now.year - args.delta_back, 1, 1)
    bgn_prd = bgn_prd + relativedelta(days=7 - bgn_prd.weekday())

    bars_in_period = BarsInPeriodProvider(conn=con, interval_len=300, interval_type='s', bars_table='bars_5m', bgn_prd=bgn_prd, delta=relativedelta(weeks=1),
                                          overlap=relativedelta(microseconds=-1))

    for i, df in enumerate(bars_in_period):
        logging.info('Saving ' + bars_in_period.current_cache_key())
        write(bars_in_period.current_cache_key(), df, lmdb_path)
