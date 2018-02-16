"""
Script that updates the splits/dividends/fundamentals cache
"""

import argparse
import logging

import atpy.data.iqfeed.util as iqutil
from atpy.data.cache.influxdb_cache import ClientFactory
from atpy.data.iqfeed.iqfeed_influxdb_cache import IQFeedInfluxDBCache
from atpy.data.iqfeed.iqfeed_level_1_provider import IQFeedLevel1Listener
from atpy.data.iqfeed.iqfeed_level_1_provider import get_fundamentals

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="InfluxDB and IQFeed configuration")

    parser.add_argument('-host', type=str, default='localhost', help="InfluxDB location host")
    parser.add_argument('-port', type=int, default=8086, help="InfluxDB host port")
    parser.add_argument('-user', type=str, default='root', help="InfluxDB username")
    parser.add_argument('-password', type=str, default='root', help="InfluxDB password")
    parser.add_argument('-drop', action='store_true', help="Drop the measurements")
    parser.add_argument('-database', type=str, default='cache', help="InfluxDB database name")
    parser.add_argument('-update_fundamentals', default=True, help="Update Fundamental data")
    parser.add_argument('-update_splits_dividends', default=True, help="Update Splits and dividends")
    parser.add_argument('-symbols_file', type=str, default=None, help="location to locally saved symbols file (to prevent downloading it every time)")
    args = parser.parse_args()

    client_factory = ClientFactory(host=args.host, port=args.port, username=args.user, password=args.password, database=args.database, pool_size=1)
    client = client_factory.new_client()

    logging.getLogger(__name__).info("Updating database with arguments: " + str(args))

    if args.drop:
        client.drop_measurement('iqfeed_fundamentals')
        client.query('DELETE FROM splits_dividends WHERE data_provider="iqfeed"')

    if args.database not in [d['name'] for d in client.get_list_database()]:
        client.create_database(args.database)
        client.query("ALTER RETENTION POLICY autogen ON cache DURATION INF REPLICATION 1 SHARD DURATION 2600w DEFAULT")

    client.switch_database(args.database)

    with IQFeedLevel1Listener(fire_ticks=False) as listener, \
            IQFeedInfluxDBCache(client_factory=client_factory, use_stream_events=False) as cache:
        all_symbols = set(iqutil.get_symbols(symbols_file=args.symbols_file).keys())

        fundamentals = get_fundamentals(all_symbols)

        if args.update_fundamentals:
            cache.update_fundamentals(fundamentals=fundamentals.values())

        if args.update_splits_dividends:
            cache.update_splits_dividends(fundamentals=fundamentals.values())

    client.close()
