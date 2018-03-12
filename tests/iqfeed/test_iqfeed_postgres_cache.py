import unittest

import datetime
import pandas as pd
import psycopg2
from atpy.data.iqfeed.iqfeed_history_provider import BarsInPeriodFilter, IQFeedHistoryProvider
from dateutil.relativedelta import relativedelta
from pandas.util.testing import assert_frame_equal
from sqlalchemy import create_engine

from atpy.data.cache.postgres_cache import update_to_latest, create_bars, bars_indices, request_bars, BarsInPeriodProvider
from atpy.data.iqfeed.iqfeed_postgres_cache import noncache_provider
from dateutil import tz


class TestInfluxDBCache(unittest.TestCase):
    """
    Test InfluxDBCache
    """

    def test_update_to_latest(self):
        with IQFeedHistoryProvider(num_connections=2) as history:
            try:
                table_name = 'bars_test'

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

                filters_no_limit = (BarsInPeriodFilter(ticker="IBM", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                                    BarsInPeriodFilter(ticker="AAPL", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                                    BarsInPeriodFilter(ticker="AMZN", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'))

                data = [history.request_data(f, sync_timestamps=False, adjust_data=False) for f in filters]

                for datum, f in zip(data, filters):
                    del datum['timestamp']
                    datum['symbol'] = f.ticker
                    datum['interval'] = '3600_s'
                    datum = datum.tz_localize(None)
                    datum.to_sql(table_name, con=engine, if_exists='append')

                latest_old = pd.read_sql("select symbol, max(timestamp) as timestamp from {0} group by symbol".format(table_name), con=con, index_col=['symbol'])['timestamp']

                update_to_latest(url=url, bars_table=table_name, symbols={('AAPL', 3600, 's'), ('AMZN', 3600, 's')}, noncache_provider=noncache_provider(history), time_delta_back=relativedelta(years=10))

                latest_current = pd.read_sql("select symbol, max(timestamp) as timestamp from {0} group by symbol".format(table_name), con=con, index_col=['symbol'])['timestamp']

                self.assertEqual(len(latest_current), len(latest_old) + 1)
                self.assertEqual(len([k for k in latest_current.keys() & latest_old.keys()]) + 1, len(latest_current))
                for k in latest_current.keys() & latest_old.keys():
                    self.assertGreater(latest_current[k], latest_old[k])

                data_no_limit = [history.request_data(f, sync_timestamps=False, adjust_data=False) for f in filters_no_limit]
                cache_data_no_limit = [request_bars(conn=engine, bars_table=table_name, interval_len=3600, interval_type='s', symbol=f.ticker,
                                                    bgn_prd=f.bgn_prd.astimezone(tz.tzutc()) + relativedelta(microseconds=1)) for f in filters_no_limit]
                for df1, df2 in zip(data_no_limit, cache_data_no_limit):
                    del df1['timestamp']
                    del df1['symbol']
                    del df1['close']
                    del df2['close']
                    assert_frame_equal(df1, df2)
            finally:
                con.cursor().execute("DROP TABLE IF EXISTS {0};".format(table_name))

    def test_bars_in_period(self):
        with IQFeedHistoryProvider(num_connections=2) as history:
            try:
                table_name = 'bars_test'

                url = 'postgresql://postgres:postgres@localhost:5432/test'

                engine = create_engine(url)
                con = psycopg2.connect(url)
                con.autocommit = True

                cur = con.cursor()

                cur.execute(create_bars.format(table_name))
                cur.execute(bars_indices.format(table_name))

                now = datetime.datetime.now()
                bgn_prd = datetime.datetime(now.year - 1, 3, 1).astimezone(tz.gettz('US/Eastern'))
                filters = (BarsInPeriodFilter(ticker="IBM", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'),
                           BarsInPeriodFilter(ticker="AAPL", bgn_prd=bgn_prd, end_prd=None, interval_len=3600, ascend=True, interval_type='s'))

                data = [history.request_data(f, sync_timestamps=False, adjust_data=False) for f in filters]

                for datum, f in zip(data, filters):
                    del datum['timestamp']
                    datum['symbol'] = f.ticker
                    datum['interval'] = '3600_s'
                    datum = datum.tz_localize(None)
                    datum.to_sql(table_name, con=engine, if_exists='append')

                now = datetime.datetime.now()

                # test all symbols
                bgn_prd = datetime.datetime(now.year - 1, 3, 1, tzinfo=tz.gettz('UTC'))
                bars_in_period = BarsInPeriodProvider(conn=con, interval_len=3600, interval_type='s', bars_table=table_name, bgn_prd=bgn_prd, delta=relativedelta(days=30), overlap=relativedelta(microseconds=-1))

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
                con.cursor().execute("DROP TABLE IF EXISTS bars_test;")


if __name__ == '__main__':
    unittest.main()
