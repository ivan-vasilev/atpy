import unittest

import psycopg2
from sqlalchemy import create_engine

from atpy.data.cache.postgres_cache import BarsBySymbolProvider, create_bars, bars_indices, create_json_data, insert_df_json, request_adjustments
from atpy.data.iqfeed.iqfeed_history_provider import *
from atpy.data.splits_dividends import exclude_splits
from atpy.ml.labeling import triple_barriers
from atpy.ml.util import *


class TestDataPipeline(unittest.TestCase):

    @staticmethod
    def __generate_temp_pipeline(url):
        with IQFeedHistoryProvider(num_connections=2) as history:
            # historical data
            engine = create_engine(url)
            con = psycopg2.connect(url)
            con.autocommit = True

            cur = con.cursor()

            cur.execute(create_bars.format('bars_test'))
            cur.execute(bars_indices.format('bars_test'))

            filters = (BarsFilter(ticker="IBM", interval_len=3600, interval_type='s', max_bars=10000),
                       BarsFilter(ticker="AAPL", interval_len=3600, interval_type='s', max_bars=10000),
                       BarsFilter(ticker="MSFT", interval_len=3600, interval_type='s', max_bars=10000),
                       BarsFilter(ticker="FB", interval_len=3600, interval_type='s', max_bars=10000),
                       BarsFilter(ticker="GOOG", interval_len=3600, interval_type='s', max_bars=10000))

            data = [history.request_data(f, sync_timestamps=False) for f in filters]

            for datum, f in zip(data, filters):
                del datum['timestamp']
                del datum['total_volume']
                del datum['number_of_trades']
                datum['symbol'] = f.ticker
                datum['interval'] = '3600_s'

                datum = datum.tz_localize(None)
                datum.to_sql('bars_test', con=engine, if_exists='append')

            # adjustments
            adjustments = get_splits_dividends({'IBM', 'AAPL', 'MSFT', 'FB', 'GOOG'})

            cur = con.cursor()

            cur.execute(create_json_data.format('json_data_test'))

            insert_df_json(con, 'json_data_test', adjustments)

    def test_data_generation_pipeline(self):
        url = 'postgresql://postgres:postgres@localhost:5432/test'
        con = psycopg2.connect(url)
        con.autocommit = True

        try:
            self.__generate_temp_pipeline(url)
            bars_per_symbol = BarsBySymbolProvider(conn=con, records_per_query=50000, interval_len=3600, interval_type='s', table_name='bars_test')

            for df in bars_per_symbol:
                orig = df
                df['pt'] = 0.001
                df['sl'] = 0.001

                adj = request_adjustments(con, 'json_data_test', symbol=list(df.index.get_level_values('symbol').unique()), adj_type='split')
                self.assertTrue(adj.size > 0)

                df = triple_barriers(df['close'], df['pt'], sl=df['sl'], vb=pd.Timedelta('36000s'), parallel=False)
                self.assertTrue(df.size > 0)

                df['include'] = True
                df['include'] = exclude_splits(df['include'], adj['value'].xs('split', level='type'), 10)
                self.assertTrue(df['include'].max())
                self.assertFalse(df['include'].min())

                tmp = orig['close'].to_frame()
                tmp['threshold'] = 0.02
                to_include = cumsum_filter(tmp, parallel=True)
                df.loc[~df.index.isin(to_include), 'include'] = False

                self.assertTrue(df['include'].max())
                self.assertFalse(df['include'].min())

                df['frac_diff'] = frac_diff_ffd(orig['close'], 0.4)
                pass
        finally:
            con.cursor().execute("DROP TABLE IF EXISTS bars_test;")
            con.cursor().execute("DROP TABLE IF EXISTS json_data_test;")
