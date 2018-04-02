#!/bin/python3
"""
Script that updates the bars/splits/dividends/fundamentals cache
"""

import argparse
import logging
import os

import psycopg2

import atpy.data.iqfeed.util as iqutil
from atpy.data.cache.postgres_cache import create_adjustments, adjustments_indices, insert_df
from atpy.data.iqfeed.iqfeed_level_1_provider import get_splits_dividends, IQFeedLevel1Listener
from pyevents.events import SyncListeners

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="PostgreSQL and IQFeed configuration")

    parser.add_argument('-url', type=str, default=os.environ['POSTGRESQL_CACHE'], help="PostgreSQL connection string")
    parser.add_argument('-symbols_file', type=str, default=None, help="location to locally saved symbols file (to prevent downloading it every time)")

    args = parser.parse_args()

    con = psycopg2.connect(args.url)
    con.autocommit = True

    all_symbols = set(iqutil.get_symbols(symbols_file=args.symbols_file).keys())

    with IQFeedLevel1Listener(listeners=SyncListeners(), fire_ticks=False) as listener:
        adjustments = get_splits_dividends(all_symbols, listener.conn)

        table_name = 'splits_dividends'
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS {0};".format(table_name))
        cur.execute(create_adjustments.format(table_name))
        cur.execute(adjustments_indices.format(table_name))

        insert_df(con, table_name, adjustments)
