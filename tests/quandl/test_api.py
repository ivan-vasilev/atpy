import unittest

import datetime
import pandas as pd
import psycopg2
from atpy.backtesting.data_replay import DataReplay
from influxdb import DataFrameClient
from pandas.util.testing import assert_frame_equal
from sqlalchemy import create_engine

from atpy.data.quandl.api import QuandlEvents, get_sf, bulkdownload_sf
from atpy.data.quandl.influxdb_cache import InfluxDBCache
from atpy.data.quandl.postgres_cache import bulkinsert_SF0, request_sf, SFInPeriodProvider
from pyevents.events import SyncListeners
from dateutil.relativedelta import relativedelta


class TestQuandlAPI(unittest.TestCase):

    def test_1(self):
        listeners = SyncListeners()
        QuandlEvents(listeners)

        results = list()

        def listener(event):
            if event['type'] == 'quandl_timeseries_result':
                results.append(event['data'])

        listeners += listener

        listeners({'type': 'quandl_fundamentals_request',
                   'data': [{'dataset': 'SF1/NKE_GP_MRQ'}, {'dataset': 'SF1/AAPL_GP_MRQ'}],
                   'threads': 1,
                   'async': False})

        data = results[0]

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)
        self.assertEqual(len(data.index.levels), 4)

        listeners({'type': 'quandl_fundamentals_request',
                   'data': [{'dataset': 'SF1/NKE_GP_MRQ'}],
                   'threads': 1,
                   'async': False})

        data = results[0]

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)
        self.assertEqual(len(data.index.levels), 4)

    def test_bulkdownload(self):
        called = False
        for data in bulkdownload_sf():
            called = True
            self.assertTrue(isinstance(data, pd.DataFrame))
            self.assertGreater(len(data), 0)
            self.assertEqual(len(data.index.levels), 4)

        self.assertTrue(called)

    def test_influxdb_cache(self):
        client = DataFrameClient(host='localhost', port=8086, username='root', password='root', database='test_cache')

        try:
            client.drop_database('test_cache')
            client.create_database('test_cache')
            client.switch_database('test_cache')

            with InfluxDBCache(client=client) as cache:
                cache.add_sf_to_cache()
                data = cache.request_data('SF0', tags={'symbol': {'AAPL', 'IBM'}})

                listeners = SyncListeners()
                QuandlEvents(listeners)

                self.assertTrue(isinstance(data, pd.DataFrame))
                self.assertGreater(len(data), 0)
                self.assertEqual(len(data.index.levels), 4)

                non_cache_data = get_sf([{'dataset': 'SF0/NKE_GP_MRY'}, {'dataset': 'SF0/MSFT_GP_MRY'}])
                cache_data = cache.request_data('SF0', tags={'symbol': {'NKE', 'MSFT'}, 'dimension': 'MRY', 'indicator': 'GP'})
                assert_frame_equal(non_cache_data, cache_data)
        finally:
            client.drop_database('test_cache')
            client.close()

    def test_postgres_cache(self):
        table_name = 'quandl_sf0'
        url = 'postgresql://postgres:postgres@localhost:5432/test'
        con = psycopg2.connect(url)
        con.autocommit = True

        try:
            engine = create_engine(url)

            bulkinsert_SF0(url, table_name=table_name)

            df = request_sf(conn=engine, symbol=['AAPL', 'IBM'])

            self.assertTrue(set(df.index.levels[1]) == {'AAPL', 'IBM'})

            df = request_sf(conn=engine, symbol=['AAPL', 'IBM'], bgn_prd=datetime.datetime(year=2017, month=1, day=1), end_prd=datetime.datetime.now())

            self.assertTrue(set(df.index.levels[1]) == {'AAPL', 'IBM'})
            self.assertEqual(min(df.index.levels[0]).year, 2017)
        finally:
            con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))

    def test_in_period_provider(self):
        table_name = 'quandl_sf0'
        url = 'postgresql://postgres:postgres@localhost:5432/test'
        con = psycopg2.connect(url)
        con.autocommit = True

        try:
            engine = create_engine(url)

            bulkinsert_SF0(url, table_name=table_name)

            now = datetime.datetime.now()
            bars_in_period = SFInPeriodProvider(conn=engine, bgn_prd=datetime.datetime(year=now.year - 5, month=1, day=1), delta=relativedelta(years=1), overlap=relativedelta(microseconds=-1))
            for df in bars_in_period:
                self.assertEqual(len(df.index.levels), 4)
                self.assertFalse(df.empty)
        finally:
            con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))

    def test_data_replay(self):
        table_name = 'quandl_sf0'
        url = 'postgresql://postgres:postgres@localhost:5432/test'
        con = psycopg2.connect(url)
        con.autocommit = True

        dates = set()
        try:
            engine = create_engine(url)

            bulkinsert_SF0(url, table_name=table_name)

            now = datetime.datetime.now()
            bars_in_period = SFInPeriodProvider(conn=engine, bgn_prd=datetime.datetime(year=now.year - 10, month=1, day=1), delta=relativedelta(years=1), overlap=relativedelta(microseconds=-1))

            dr = DataReplay().add_source(iter(bars_in_period), 'e1', historical_depth=2)

            for i, r in enumerate(dr):
                for e in r:
                    d = r[e].iloc[-1].name[0].to_pydatetime()

                if len(dates) > 0:
                    self.assertGreater(d, max(dates))

                dates.add(d)

                self.assertTrue(isinstance(r, dict))
                self.assertGreaterEqual(len(r), 1)
        finally:
            con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))


if __name__ == '__main__':
    unittest.main()
