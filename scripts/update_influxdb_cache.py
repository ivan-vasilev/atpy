#!/bin/python3

"""
Script that populates the InfluxDB cache initially and the updates it incrementally
"""

import argparse
import logging

from dateutil.relativedelta import relativedelta
from influxdb import DataFrameClient

import atpy.data.iqfeed.util as iqutil
from atpy.data.cache.influxdb_cache import update_to_latest
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider
from atpy.data.iqfeed.iqfeed_influxdb_cache import noncache_provider

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="InfluxDB and IQFeed configuration")

    parser.add_argument('-host', type=str, default='localhost', help="InfluxDB location host")
    parser.add_argument('-port', type=int, default=8086, help="InfluxDB host port")
    parser.add_argument('-user', type=str, default='root', help="InfluxDB username")
    parser.add_argument('-password', type=str, default='root', help="InfluxDB password")
    parser.add_argument('-database', type=str, default='cache', help="InfluxDB database name")
    parser.add_argument('-drop', action='store_true', help="Drop the database")
    parser.add_argument('-skip_if_older', type=int, default=None, help="Skip symbols, which are in the database, but have no activity for more than N previous days")
    parser.add_argument('-interval_len', type=int, default=None, required=True, help="Interval length")
    parser.add_argument('-interval_type', type=str, default='s', help="Interval type (seconds, days, etc)")
    parser.add_argument('-iqfeed_conn', type=int, default=10, help="Number of historical connections to IQFeed")
    parser.add_argument('-delta_back', type=int, default=10, help="Default number of years to look back")
    parser.add_argument('-symbols_file', type=str, default=None, help="location to locally saved symbols file (to prevent downloading it every time)")
    args = parser.parse_args()

    client = DataFrameClient(host=args.host, port=args.port, username=args.user, password=args.password, database=args.database, pool_size=1)

    logging.getLogger(__name__).info("Updating database with arguments: " + str(args))

    if args.drop:
        client.drop_database(args.database)

    if args.database not in [d['name'] for d in client.get_list_database()]:
        client.create_database(args.database)
        client.query("ALTER RETENTION POLICY autogen ON cache DURATION INF REPLICATION 1 SHARD DURATION 2600w DEFAULT")

    client.switch_database(args.database)

    with IQFeedHistoryProvider(num_connections=args.iqfeed_conn) as history:
        all_symbols = {(s, args.interval_len, args.interval_type) for s in set(iqutil.get_symbols(symbols_file=args.symbols_file).keys())}
        update_to_latest(client=client, noncache_provider=noncache_provider(history), new_symbols=all_symbols, time_delta_back=relativedelta(years=args.delta_back),
                         skip_if_older_than=relativedelta(days=args.skip_if_older) if args.skip_if_older is not None else None)

    client.close()
