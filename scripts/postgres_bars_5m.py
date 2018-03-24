import argparse
import logging
import os

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="PostgreSQL and IQFeed configuration")
    parser.add_argument('-drop', action='store_true', help="Drop the database")
    args = parser.parse_args()

    query = "python3 ../atpy/data/iqfeed/update_postgres_cache.py " + \
            ("-drop" if args.drop else "") + \
            " -url='" + os.environ['POSTGRESQL_CACHE'] + "'" + \
            " -update_bars" + \
            " -table_name='bars_5m'" + \
            " -interval_len=300" + \
            " -interval_type='s'" + \
            " -skip_if_older=30"

    os.system(query)
