import argparse
import logging
import os

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="PostgreSQL and IQFeed configuration")
    parser.add_argument('-drop', action='store_true', help="Drop the database")
    parser.add_argument('-cluster', action='store_true', help="Cluster the table after inserts")
    args = parser.parse_args()

    query = "python3 ../atpy/data/iqfeed/update_postgres_cache.py " + \
            ("-drop" if args.drop else "") + \
            (" -cluster" if args.cluster else "") + \
            " -url='" + os.environ['POSTGRESQL_CACHE'] + "'" + \
            " -update_bars" + \
            " -table_name='bars_60m'" + \
            " -interval_len=3600" + \
            " -interval_type='s'"

    os.system(query)
