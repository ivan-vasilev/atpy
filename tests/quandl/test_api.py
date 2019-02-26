import datetime
import unittest

import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta
from influxdb import DataFrameClient, InfluxDBClient
from pandas.util.testing import assert_frame_equal
from sqlalchemy import create_engine

from atpy.backtesting.data_replay import DataReplay
from atpy.data.quandl.api import QuandlEvents, bulkdownload, get_table
from atpy.data.quandl.influxdb_cache import InfluxDBCache
from atpy.data.quandl.postgres_cache import bulkinsert_SF0, request_sf, SFInPeriodProvider
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
                if isinstance(event['data']['SHARADAR/SF1'], list):
                    results.extend(event['data']['SHARADAR/SF1'])
                elif isinstance(event['data']['SHARADAR/SF1'], pd.DataFrame):
                    results.append(event['data']['SHARADAR/SF1'])

        listeners += listener

        listeners({'type': 'quandl_table_request',
                   'data': [{'datatable_code': 'SHARADAR/SF1', 'ticker': 'AAPL', 'dimension': 'MRY', 'qopts': {"columns": ['ticker', 'dimension', 'datekey', 'revenue']}},
                            {'datatable_code': 'SHARADAR/SF1', 'ticker': 'IBM', 'dimension': 'MRY', 'qopts': {"columns": ['ticker', 'dimension', 'datekey', 'revenue']}}],
                   'threads': 1,
                   'async': False})

        data = pd.concat(results)

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)
        results.clear()

        listeners({'type': 'quandl_table_request',
                   'data': [{'datatable_code': 'SHARADAR/SF1', 'ticker': 'AAPL', 'dimension': 'MRY'}],
                   'threads': 1,
                   'async': False})

        data = results[0]

        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)

    def test_influxdb_cache(self):
        client = InfluxDBClient(host='localhost', port=8086, username='root', password='root', database='test_cache')

        try:
            client.drop_database('test_cache')
            client.create_database('test_cache')
            client.switch_database('test_cache')

            with InfluxDBCache(client=DataFrameClient(host='localhost', port=8086, username='root', password='root', database='test_cache')) as cache:
                listeners = SyncListeners()
                QuandlEvents(listeners)

                non_cache_data = get_table([{'datatable_code': 'SHARADAR/SF1', 'ticker': 'AAPL', 'dimension': 'MRY', 'qopts': {"columns": ['dimension', 'ticker', 'datekey', 'revenue']}},
                                            {'datatable_code': 'SHARADAR/SF1', 'ticker': 'IBM', 'dimension': 'MRY', 'qopts': {"columns": ['dimension', 'ticker', 'datekey', 'revenue']}}])

                items = list()
                for df in non_cache_data['SHARADAR/SF1']:
                    items.append(df.rename({'revenue': 'value', 'datekey': 'timestamp'}, axis=1).set_index('timestamp'))

                cache.add_to_cache('sf1', iter(items), tag_columns=['dimension', 'ticker'])
                cache_data = cache.request_data('sf1', tags={'ticker': {'AAPL', 'IBM'}})

                listeners = SyncListeners()
                QuandlEvents(listeners)

                self.assertTrue(isinstance(cache_data, pd.DataFrame))
                self.assertGreater(len(cache_data), 0)

                items = pd.concat(items).sort_index()
                items = items.tz_localize('UTC', copy=False)
                items.index.name = 'date'

                assert_frame_equal(items, cache_data)
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

            df = request_sf(conn=engine, table_name=table_name, symbol=['AAPL', 'TSLA'])

            self.assertTrue(set(df.index.levels[1]) == {'AAPL', 'TSLA'})

            df = request_sf(conn=engine, symbol=['AAPL', 'TSLA'], bgn_prd=datetime.datetime(year=2017, month=1, day=1), end_prd=datetime.datetime.now())

            self.assertTrue(set(df.index.levels[1]) == {'AAPL', 'TSLA'})
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
            bars_in_period = SFInPeriodProvider(conn=engine, table_name=table_name, bgn_prd=datetime.datetime(year=now.year - 5, month=1, day=1), delta=relativedelta(years=1), overlap=relativedelta(microseconds=-1))

            called = False

            for df in bars_in_period:
                called = True
                self.assertEqual(len(df.index.levels), 4)
                self.assertFalse(df.empty)

            self.assertTrue(called)
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
            bars_in_period = SFInPeriodProvider(conn=engine, table_name=table_name, bgn_prd=datetime.datetime(year=now.year - 10, month=1, day=1), delta=relativedelta(years=1), overlap=relativedelta(microseconds=-1))

            dr = DataReplay().add_source(bars_in_period, 'e1', historical_depth=2)

            for i, r in enumerate(dr):
                d = r['e1'].iloc[-1].name[0].to_pydatetime()

                if len(dates) > 0:
                    self.assertGreater(d, max(dates))

                dates.add(d)

                self.assertTrue(isinstance(r, dict))
                self.assertGreaterEqual(len(r), 1)
        finally:
            con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))

    def test_bulkdownload(self):
        data = bulkdownload("SF0")
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertGreater(len(data), 0)

        called = False
        for data in bulkdownload("SF0", chunksize=100000000):
            called = True
            self.assertTrue(isinstance(data, pd.DataFrame))
            self.assertGreater(len(data), 0)

        self.assertTrue(called)


if __name__ == '__main__':
    unittest.main()
