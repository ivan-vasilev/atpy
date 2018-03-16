#!/bin/python3
"""
Script that updates the bars/splits/dividends/fundamentals cache
"""

import argparse
import logging
import typing

import psycopg2
import sqlalchemy
from dateutil.relativedelta import relativedelta

import atpy.data.iqfeed.util as iqutil
from atpy.data.cache.postgres_cache import update_to_latest, create_adjustments, adjustments_indices, insert_df
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider
from atpy.data.iqfeed.iqfeed_level_1_provider import get_splits_dividends, get_fundamentals, IQFeedLevel1Listener
from atpy.data.iqfeed.iqfeed_postgres_cache import noncache_provider
from pyevents.events import SyncListeners

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="PostgreSQL and IQFeed configuration")

    parser.add_argument('-url', type=str, default=None, help="PostgreSQL connection string")
    parser.add_argument('-drop', action='store_true', help="Drop the table")
    parser.add_argument('-table_name', type=str, default=None, required=True, help="PostgreSQL database name")

    group = parser.add_mutually_exclusive_group()

    group.add_argument('-update_bars', action='store_true', help="Update Fundamental data")
    group.add_argument('-update_fundamentals', action='store_true', help="Update Fundamental data")
    group.add_argument('-update_splits_dividends', action='store_true', help="Update Splits and dividends")

    parser.add_argument('-interval_len', type=int, default=None, help="Interval length")
    parser.add_argument('-interval_type', type=str, default='s', help="Interval type (seconds, days, etc)")
    parser.add_argument('-skip_if_older', type=int, default=None, help="Skip symbols, which are in the database, but have no activity for more than N previous days")
    parser.add_argument('-delta_back', type=int, default=10, help="Default number of years to look back")
    parser.add_argument('-iqfeed_conn', type=int, default=10, help="Number of historical connections to IQFeed")

    parser.add_argument('-symbols_file', type=str, default=None, help="location to locally saved symbols file (to prevent downloading it every time)")

    args = parser.parse_args()

    con = psycopg2.connect(args.url)
    con.autocommit = True

    if args.update_bars is True:
        if args.drop:
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS {0};".format(args.table_name))

        if args.interval_len is None or args.interval_type is None:
            parser.error('-interval_len and -interval_type are required')

        with IQFeedHistoryProvider(num_connections=args.iqfeed_conn) as history:
            all_symbols = set((s, args.interval_len, args.interval_type) for s in set(iqutil.get_symbols(symbols_file=args.symbols_file).keys()))
            update_to_latest(url=args.url, bars_table=args.table_name, noncache_provider=noncache_provider(history), symbols=all_symbols, time_delta_back=relativedelta(years=args.delta_back),
                             skip_if_older_than=relativedelta(days=args.skip_if_older) if args.skip_if_older is not None else None)

    if args.update_fundamentals or args.update_splits_dividends:
        all_symbols = set(iqutil.get_symbols(symbols_file=args.symbols_file).keys())

        if args.update_splits_dividends:
            with IQFeedLevel1Listener(listeners=SyncListeners(), fire_ticks=False) as listener:
                adjustments = get_splits_dividends(all_symbols, listener.conn)

                cur = con.cursor()
                cur.execute("DROP TABLE IF EXISTS {0};".format(args.table_name))
                cur.execute(create_adjustments.format(args.table_name))
                cur.execute(adjustments_indices.format(args.table_name))

                insert_df(con, args.table_name, adjustments)

        if args.update_fundamentals:
            with IQFeedLevel1Listener(listeners=SyncListeners(), fire_ticks=False) as listener:
                fundamentals = get_fundamentals(all_symbols, listener.conn)

