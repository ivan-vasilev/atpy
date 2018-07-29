import datetime
import unittest

import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta
from influxdb import DataFrameClient
from pandas.util.testing import assert_frame_equal
from sqlalchemy import create_engine

from atpy.backtesting.data_replay import DataReplay
from atpy.data.quandl.api import QuandlEvents, get_sf1, bulkdownload
from atpy.data.quandl.influxdb_cache import InfluxDBCache
from atpy.data.quandl.postgres_cache import request_sf, SFInPeriodProvider
from pyevents.events import SyncListeners


class TestQuandlAPI(unittest.TestCase):

    def test_time_series_1(self):
        listeners = SyncListeners()
        QuandlEvents(listeners)

        results = list()

        def listener(event):
            if event['type'] == 'quandl_timeseries_result':
                results.append(event['data'])

        listeners += listener

        listeners({'type': 'quandl_timeseries_request',
                   'data': [{'dataset': 'WWDI/SSD_SL_EMP_VULN_ZS'}, {'dataset': 'WWDI/SSD_SL_EMP_WORK_ZS'}],
                   'threads': 1,
                   'async': False})

        data = pd.concat(results[0])

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertTrue(isinstance(data.index, pd.MultiIndex))

        self.assertGreater(len(data), 0)
        results.clear()

        listeners({'type': 'quandl_timeseries_request',
                   'data': [{'dataset': 'WWDI/SSD_SL_EMP_VULN_ZS'}],
                   'threads': 1,
                   'async': False})

        data = next(iter(results[0].values()))

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)

    def test_tables_1(self):
        listeners = SyncListeners()
        QuandlEvents(listeners)

        results = list()

        def listener(event):
            if event['type'] == 'quandl_table_result':
                results.append(event['data'])

        listeners += listener

        listeners({'type': 'quandl_table_request',
                   'data': [{'datatable_code': 'SHARADAR/SF1', 'ticker': 'AAPL', 'dimension': 'MRY', 'qopts': {"columns": ['ticker', 'dimension', 'datekey', 'revenue']}},
                            {'datatable_code': 'SHARADAR/SF1', 'ticker': 'IBM', 'dimension': 'MRY', 'qopts': {"columns": ['ticker', 'dimension', 'datekey', 'revenue']}}],
                   'threads': 1,
                   'async': False})

        data = pd.concat(results[0])

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertTrue(isinstance(data.index, pd.MultiIndex))

        self.assertGreater(len(data), 0)
        results.clear()

        listeners({'type': 'quandl_table_request',
                   'data': [{'datatable_code': 'SHARADAR/SF1', 'ticker': 'AAPL', 'dimension': 'MRY'}],
                   'threads': 1,
                   'async': False})

        data = next(iter(results[0].values()))

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)

    # TODO
    def test_bulkdownload(self):
        called = False
        for data in bulkdownload("ZEA"):
            called = True
            self.assertTrue(isinstance(data, pd.DataFrame))
            self.assertGreater(len(data), 0)

        self.assertTrue(called)

    # TODO
    def test_influxdb_cache(self):
        client = DataFrameClient(host='localhost', port=8086, username='root', password='root', database='test_cache')

        try:
            client.drop_database('test_cache')
            client.create_database('test_cache')
            client.switch_database('test_cache')

            with InfluxDBCache(client=client) as cache:
                cache.add_dataset_to_cache()
                data = cache.request_data('WGLF', tags={'symbol': {'AAPL', 'IBM'}})

                listeners = SyncListeners()
                QuandlEvents(listeners)

                self.assertTrue(isinstance(data, pd.DataFrame))
                self.assertGreater(len(data), 0)
                self.assertEqual(len(data.index.levels), 4)

                non_cache_data = get_sf1([{'dataset': 'WGLF/TZA_WP11632_8'}, {'dataset': 'WGLF/PSE_WP11670_2'}])
                cache_data = cache.request_data('WGLF', tags={'symbol': 'PSE', 'dimension': '2', 'indicator': 'WP11670'})
                assert_frame_equal(non_cache_data, cache_data)
        finally:
            client.drop_database('test_cache')
            client.close()

    # TODO
    def test_postgres_cache(self):
        table_name = 'quandl_WGLF'
        url = 'postgresql://postgres:postgres@localhost:5432/test'
        con = psycopg2.connect(url)
        con.autocommit = True

        try:
            engine = create_engine(url)

            # TODO
            # bulkinsert_SF0(url, table_name=table_name)

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

            # TODO
            # bulkinsert_SF0(url, table_name=table_name)

            now = datetime.datetime.now()
            bars_in_period = SFInPeriodProvider(conn=engine, bgn_prd=datetime.datetime(year=now.year - 5, month=1, day=1), delta=relativedelta(years=1), overlap=relativedelta(microseconds=-1))
            for df in bars_in_period:
                self.assertEqual(len(df.index.levels), 4)
                self.assertFalse(df.empty)
        finally:
            con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))

    # TODO
    def test_data_replay(self):
        table_name = 'quandl_sf0'
        url = 'postgresql://postgres:postgres@localhost:5432/test'
        con = psycopg2.connect(url)
        con.autocommit = True

        dates = set()
        try:
            engine = create_engine(url)
            # TODO
            # bulkinsert_SF0(url, table_name=table_name)

            now = datetime.datetime.now()
            bars_in_period = SFInPeriodProvider(conn=engine, bgn_prd=datetime.datetime(year=now.year - 10, month=1, day=1), delta=relativedelta(years=1), overlap=relativedelta(microseconds=-1))

            dr = DataReplay().add_source(bars_in_period, 'e1', historical_depth=2)

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
