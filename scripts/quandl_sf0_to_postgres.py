#!/bin/python3

import os

import psycopg2
from sqlalchemy import create_engine

from atpy.data.quandl.postgres_cache import bulkinsert_SF0

if __name__ == "__main__":
    table_name = 'quandl_sf0'
    url = os.environ['POSTGRESQL_CACHE']
    con = psycopg2.connect(url)
    con.autocommit = True

    engine = create_engine(url)

    bulkinsert_SF0(url, table_name=table_name)
