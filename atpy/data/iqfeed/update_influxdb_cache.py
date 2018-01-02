"""
Script that populates the InfluxDB cache initially and the updates it incrementally
"""

import argparse
import datetime
import logging

from dateutil.relativedelta import relativedelta

import atpy.data.iqfeed.util as iqutil
from atpy.data.cache.influxdb_cache import ClientFactory
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryProvider
from atpy.data.iqfeed.iqfeed_influxdb_cache import IQFeedInfluxDBCache

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

    client_factory = ClientFactory(host=args.host, port=args.port, username=args.user, password=args.password, database=args.database, pool_size=1)
    client = client_factory.new_client()

    logging.getLogger(__name__).info("Updating database with arguments: " + str(args))

    if args.drop:
        client.drop_database(args.database)

    if args.database not in [d['name'] for d in client.get_list_database()]:
        client.create_database(args.database)
        client.query("ALTER RETENTION POLICY autogen ON cache DURATION INF REPLICATION 1 SHARD DURATION 2600w DEFAULT")

    client.switch_database(args.database)

    with IQFeedHistoryProvider(num_connections=args.iqfeed_conn) as history, \
            IQFeedInfluxDBCache(client_factory=client_factory, use_stream_events=False, history=history, time_delta_back=relativedelta(years=args.delta_back)) as cache:
        all_symbols = {(s, args.interval_len, args.interval_type) for s in iqutil.get_symbols(symbols_file=args.symbols_file)}
        cache.update_to_latest(all_symbols, skip_if_older_than=datetime.timedelta(days=args.skip_if_older) if args.skip_if_older is not None else None)

    client.close()
