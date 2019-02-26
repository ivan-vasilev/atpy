import shutil
import tempfile
import unittest

from pandas.util.testing import assert_frame_equal
from sqlalchemy import create_engine

import atpy.data.cache.lmdb_cache as lmdb_cache
import atpy.data.iqfeed.iqfeed_history_provider as iq_history
from atpy.data.cache.postgres_cache import *
from atpy.data.iqfeed.iqfeed_level_1_provider import get_fundamentals, get_splits_dividends
from atpy.data.iqfeed.iqfeed_postgres_cache import *


class TestPostgresCache(unittest.TestCase):
    """
    Test InfluxDBCache
    """

    def test_update_to_latest_intraday(self):
        with IQFeedHistoryProvider(num_connections=2) as history:
            table_name = 'bars_test'
            try:
                url = 'postgresql://postgres:postgres@localhost:5432/test'

                engine = create_engine(url)
                con = psycopg2.connect(url)
                con.autocommit = True

                cur = con.cursor()

                cur.execute(create_bars.format(table_name))
                cur.execute(bars_indices.format(table_name))

                bgn_prd = datetime.datetime(2017, 3, 1).astimezone(tz.gettz('US/Eastern'))
                end_prd = datetime.datetime(2017, 3, 2)
                filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=bgn_prd, end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'),
                           BarsInPeriodFilter(ticker="AAPL", bgn_prd=bgn_prd, end_prd=end_prd, interval_len=3600, ascend=True, interval_type='s'))

                data = [history.request_data(f, sync_timestamps=False) for f in filters]

                filters_no_limit = (BarsInPeriodFilter(ticker="IBM", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                                    BarsInPeriodFilter(ticker="AAPL", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                                    BarsInPeriodFilter(ticker="AMZN", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'))

                for datum, f in zip(data, filters):
                    del datum['timestamp']
                    del datum['total_volume']
                    del datum['number_of_trades']

                    datum['symbol'] = f.ticker
                    datum['interval'] = '3600_s'
                    datum = datum.tz_localize(None)
                    datum.to_sql(table_name, con=engine, if_exists='append')

                latest_old = pd.read_sql("select symbol, max(timestamp) as timestamp from {0} group by symbol".format(table_name), con=con, index_col=['symbol'])['timestamp']

                update_to_latest(url=url, bars_table=table_name, symbols={('AAPL', 3600, 's'), ('AMZN', 3600, 's')}, noncache_provider=noncache_provider(history), time_delta_back=relativedelta(years=10))
                data_no_limit = [history.request_data(f, sync_timestamps=False) for f in filters_no_limit]

                latest_current = pd.read_sql("select symbol, max(timestamp) as timestamp from {0} group by symbol".format(table_name), con=con, index_col=['symbol'])['timestamp']

                self.assertEqual(len(latest_current), len(latest_old) + 1)
                self.assertEqual(len([k for k in latest_current.keys() & latest_old.keys()]) + 1, len(latest_current))
                for k in latest_current.keys() & latest_old.keys():
                    self.assertGreater(latest_current[k], latest_old[k])

                cache_data_no_limit = [request_bars(conn=engine, bars_table=table_name, interval_len=3600, interval_type='s', symbol=f.ticker,
                                                    bgn_prd=f.bgn_prd.astimezone(tz.tzutc()) + relativedelta(microseconds=1)) for f in filters_no_limit]
                for df1, df2 in zip(data_no_limit, cache_data_no_limit):
                    del df1['timestamp']
                    del df1['total_volume']
                    del df1['number_of_trades']
                    del df1['symbol']
                    del df1['period_volume']
                    del df2['period_volume']

                    assert_frame_equal(df1, df2, check_exact=False, check_less_precise=True)
            finally:
                con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))

    def test_update_to_latest_daily(self):
        url = 'postgresql://postgres:postgres@localhost:5432/test'
        con = psycopg2.connect(url)
        con.autocommit = True

        with IQFeedHistoryProvider(num_connections=2) as history:
            table_name = 'bars_test'
            try:
                engine = create_engine(url)

                cur = con.cursor()

                cur.execute(create_bars.format(table_name))
                cur.execute(bars_indices.format(table_name))

                bgn_prd = datetime.datetime(2017, 3, 1).date()
                end_prd = datetime.datetime(2017, 3, 2).date()
                filters = (BarsDailyForDatesFilter(ticker="IBM", bgn_dt=bgn_prd, end_dt=end_prd, ascend=True),
                           BarsDailyForDatesFilter(ticker="AAPL", bgn_dt=bgn_prd, end_dt=end_prd, ascend=True))

                filters_no_limit = (BarsDailyForDatesFilter(ticker="IBM", bgn_dt=bgn_prd, end_dt=None, ascend=True),
                                    BarsDailyForDatesFilter(ticker="AAPL", bgn_dt=bgn_prd, end_dt=None, ascend=True),
                                    BarsDailyForDatesFilter(ticker="AMZN", bgn_dt=bgn_prd, end_dt=None, ascend=True))

                data = [history.request_data(f, sync_timestamps=False) for f in filters]

                for datum, f in zip(data, filters):
                    del datum['timestamp']
                    del datum['open_interest']

                    datum['symbol'] = f.ticker
                    datum['interval'] = '1_d'
                    datum = datum.tz_localize(None)
                    datum.to_sql(table_name, con=engine, if_exists='append')

                latest_old = pd.read_sql("select symbol, max(timestamp) as timestamp from {0} group by symbol".format(table_name), con=con, index_col=['symbol'])['timestamp']

                update_to_latest(url=url, bars_table=table_name, symbols={('AAPL', 1, 'd'), ('AMZN', 1, 'd')}, noncache_provider=noncache_provider(history), time_delta_back=relativedelta(years=10))

                latest_current = pd.read_sql("select symbol, max(timestamp) as timestamp from {0} group by symbol".format(table_name), con=con, index_col=['symbol'])['timestamp']

                self.assertEqual(len(latest_current), len(latest_old) + 1)
                self.assertEqual(len([k for k in latest_current.keys() & latest_old.keys()]) + 1, len(latest_current))
                for k in latest_current.keys() & latest_old.keys():
                    self.assertGreater(latest_current[k], latest_old[k])

                data_no_limit = [history.request_data(f, sync_timestamps=False) for f in filters_no_limit]

                cache_data_no_limit = [request_bars(conn=engine,
                                                    bars_table=table_name,
                                                    interval_len=1, interval_type='d',
                                                    symbol=f.ticker,
                                                    bgn_prd=datetime.datetime.combine(f.bgn_dt, datetime.datetime.min.time()).astimezone(tz.tzutc()) + relativedelta(microseconds=1)) for f in filters_no_limit]

                for df1, df2 in zip(data_no_limit, cache_data_no_limit):
                    del df1['timestamp']
                    del df1['open_interest']
                    del df1['symbol']

                    assert_frame_equal(df1, df2)
            finally:
                con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))

    def test_bars_in_period(self):
        with IQFeedHistoryProvider(num_connections=2) as history:
            tmpdir = tempfile.mkdtemp()
            table_name = 'bars_test'

            url = 'postgresql://postgres:postgres@localhost:5432/test'

            engine = create_engine(url)
            con = psycopg2.connect(url)
            con.autocommit = True

            try:
                cur = con.cursor()

                cur.execute(create_bars.format(table_name))
                cur.execute(bars_indices.format(table_name))

                now = datetime.datetime.now()
                bgn_prd = datetime.datetime(now.year - 1, 3, 1).astimezone(tz.gettz('US/Eastern'))
                filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                           BarsInPeriodFilter(ticker="AAPL", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'))

                data = [history.request_data(f, sync_timestamps=False) for f in filters]

                for datum, f in zip(data, filters):
                    del datum['timestamp']
                    del datum['total_volume']
                    del datum['number_of_trades']
                    datum['symbol'] = f.ticker
                    datum['interval'] = '3600_s'

                    datum = datum.tz_localize(None)
                    datum.to_sql(table_name, con=engine, if_exists='append')

                now = datetime.datetime.now()

                # test all symbols no cache
                bgn_prd = datetime.datetime(now.year - 1, 3, 1, tzinfo=tz.gettz('UTC'))
                bars_in_period = BarsInPeriodProvider(conn=con, interval_len=3600, interval_type='s', bars_table=table_name, bgn_prd=bgn_prd, delta=relativedelta(days=30), overlap=relativedelta(microseconds=-1))

                for i, df in enumerate(bars_in_period):
                    self.assertFalse(df.empty)

                    lmdb_cache.write(bars_in_period.current_cache_key(), df, tmpdir)

                    start, end = bars_in_period._periods[bars_in_period._deltas]
                    self.assertGreaterEqual(df.index[0][0], start)
                    self.assertGreater(end, df.index[-1][0])
                    self.assertGreater(end, df.index[0][0])

                self.assertEqual(i, len(bars_in_period._periods) - 1)
                self.assertGreater(i, 0)

                # test all symbols cache
                bgn_prd = datetime.datetime(now.year - 1, 3, 1, tzinfo=tz.gettz('UTC'))
                bars_in_period = BarsInPeriodProvider(conn=con, interval_len=3600, interval_type='s', bars_table=table_name, bgn_prd=bgn_prd, delta=relativedelta(days=30), overlap=relativedelta(microseconds=-1),
                                                      cache=functools.partial(lmdb_cache.read_pickle, lmdb_path=tmpdir))

                for i, df in enumerate(bars_in_period):
                    self.assertFalse(df.empty)

                    start, end = bars_in_period._periods[bars_in_period._deltas]
                    self.assertGreaterEqual(df.index[0][0], start)
                    self.assertGreater(end, df.index[-1][0])
                    self.assertGreater(end, df.index[0][0])

                self.assertEqual(i, len(bars_in_period._periods) - 1)
                self.assertGreater(i, 0)

                # test symbols group
                bgn_prd = datetime.datetime(now.year - 1, 3, 1, tzinfo=tz.gettz('UTC'))
                bars_in_period = BarsInPeriodProvider(conn=con, interval_len=3600, interval_type='s', bars_table=table_name, bgn_prd=bgn_prd, symbol=['AAPL', 'IBM'], delta=relativedelta(days=30), overlap=relativedelta(microseconds=-1))

                for i, df in enumerate(bars_in_period):
                    self.assertFalse(df.empty)

                    start, end = bars_in_period._periods[bars_in_period._deltas]
                    self.assertGreaterEqual(df.index[0][0], start)
                    self.assertGreater(end, df.index[-1][0])
                    self.assertGreater(end, df.index[0][0])

                self.assertEqual(i, len(bars_in_period._periods) - 1)
                self.assertGreater(i, 0)
            finally:
                shutil.rmtree(tmpdir)
                con.cursor().execute("DROP TABLE IF EXISTS bars_test;")

    def test_bars_by_symbol(self):
        with IQFeedHistoryProvider(num_connections=2) as history:
            tmpdir = tempfile.mkdtemp()
            table_name = 'bars_test'

            url = 'postgresql://postgres:postgres@localhost:5432/test'

            engine = create_engine(url)
            con = psycopg2.connect(url)
            con.autocommit = True

            try:
                cur = con.cursor()

                cur.execute(create_bars.format(table_name))
                cur.execute(bars_indices.format(table_name))

                iq_history.BarsFilter(ticker="IBM", interval_len=3600, interval_type='s', max_bars=1000)
                filters = (iq_history.BarsFilter(ticker="IBM", interval_len=3600, interval_type='s', max_bars=1000),
                           iq_history.BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=1000))

                data = [history.request_data(f, sync_timestamps=False) for f in filters]

                for datum, f in zip(data, filters):
                    del datum['timestamp']
                    del datum['total_volume']
                    del datum['number_of_trades']
                    datum['symbol'] = f.ticker
                    datum['interval'] = '3600_s'

                    datum = datum.tz_localize(None)
                    datum.to_sql(table_name, con=engine, if_exists='append')

                bars_per_symbol = BarsBySymbolProvider(conn=con, records_per_query=1000, interval_len=3600, interval_type='s', table_name=table_name)

                for i, df in enumerate(bars_per_symbol):
                    self.assertEqual(len(df), 1000)

                self.assertEqual(i, 1)

                bars_per_symbol = BarsBySymbolProvider(conn=con, records_per_query=100, interval_len=3600, interval_type='s', table_name=table_name)

                for i, df in enumerate(bars_per_symbol):
                    self.assertEqual(len(df), 1000)

                self.assertEqual(i, 1)

                bars_per_symbol = BarsBySymbolProvider(conn=con, records_per_query=2000, interval_len=3600, interval_type='s', table_name=table_name)

                for i, df in enumerate(bars_per_symbol):
                    self.assertEqual(len(df), 2000)
                    self.assertTrue(isinstance(df.index, pd.MultiIndex))

                self.assertEqual(i, 0)
            finally:
                shutil.rmtree(tmpdir)
                con.cursor().execute("DROP TABLE IF EXISTS bars_test;")

    def test_symbol_counts(self):
        with IQFeedHistoryProvider(num_connections=2) as history:
            table_name = 'bars_test'

            url = 'postgresql://postgres:postgres@localhost:5432/test'

            engine = create_engine(url)
            con = psycopg2.connect(url)
            con.autocommit = True

            try:
                cur = con.cursor()

                cur.execute(create_bars.format(table_name))
                cur.execute(bars_indices.format(table_name))

                now = datetime.datetime.now()
                bgn_prd = datetime.datetime(now.year - 1, 3, 1).astimezone(tz.gettz('US/Eastern'))
                filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                           BarsInPeriodFilter(ticker="AAPL", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'))

                data = [history.request_data(f, sync_timestamps=False) for f in filters]

                for datum, f in zip(data, filters):
                    del datum['timestamp']
                    del datum['total_volume']
                    del datum['number_of_trades']
                    datum['symbol'] = f.ticker
                    datum['interval'] = '3600_s'

                    datum = datum.tz_localize(None)
                    datum.to_sql(table_name, con=engine, if_exists='append')

                counts = request_symbol_counts(conn=con, interval_len=3600, interval_type='s', symbol=["IBM", "AAPL"], bars_table=table_name)
                self.assertEqual(counts.size, 2)
                self.assertGreater(counts.min(), 0)
            finally:
                con.cursor().execute("DROP TABLE IF EXISTS bars_test;")

    def test_update_adjustments(self):
        table_name = 'adjustments_test'
        url = 'postgresql://postgres:postgres@localhost:5432/test'

        con = psycopg2.connect(url)
        con.autocommit = True

        try:
            adjustments = get_splits_dividends({'IBM', 'AAPL', 'GOOG', 'MSFT'})

            cur = con.cursor()

            cur.execute(create_json_data.format(table_name))

            insert_df_json(con, table_name, adjustments)

            now = datetime.datetime.now()

            df = request_adjustments(con, table_name, symbol=['IBM', 'AAPL', 'MSFT', 'GOOG'], bgn_prd=datetime.datetime(year=now.year - 30, month=now.month, day=now.day),
                                     end_prd=datetime.datetime(year=now.year + 2, month=now.month, day=now.day), provider='iqfeed')

            self.assertFalse(df.empty)
            assert_frame_equal(adjustments, df)
        finally:
            con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))

    def test_update_fundamentals(self):
        table_name = 'iqfeed_fundamentals'

        url = 'postgresql://postgres:postgres@localhost:5432/test'
        con = psycopg2.connect(url)
        con.autocommit = True

        try:
            cur = con.cursor()

            cur.execute(create_json_data.format(table_name))

            fundamentals = get_fundamentals({'IBM', 'AAPL', 'GOOG', 'MSFT'})

            update_fundamentals(conn=con, fundamentals=fundamentals, table_name=table_name)

            fund = request_fundamentals(con, symbol=['IBM', 'AAPL', 'GOOG'], table_name=table_name)

            self.assertTrue(isinstance(fund, pd.DataFrame))
            self.assertEqual(len(fund), 3)
        finally:
            con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))


if __name__ == '__main__':
    unittest.main()
