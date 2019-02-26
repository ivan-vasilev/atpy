import datetime
import logging
import random
import threading
import unittest

import pandas as pd

import pyiqfeed as iq
from atpy.data.iqfeed.filters import DefaultFilterProvider
from atpy.data.iqfeed.iqfeed_history_provider import BarsInPeriodFilter, IQFeedHistoryEvents, IQFeedHistoryProvider, BarsFilter
from atpy.data.iqfeed.iqfeed_level_1_provider import get_splits_dividends
from atpy.data.splits_dividends import exclude_splits
from pyevents.events import AsyncListeners


class TestSplitsDividends(unittest.TestCase):
    """
    Test splits/dividends functionality
    """

    def test_bar_split_adjust_1(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsInPeriodFilter(ticker="PLUS", bgn_prd=datetime.datetime(2017, 3, 31), end_prd=datetime.datetime(2017, 4, 5), interval_len=3600, ascend=True, interval_type='s', max_ticks=100)

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, timestamp_first=True, num_connections=2) as listener, listener.batch_provider() as provider:
            e1 = threading.Event()

            def process_bar(event):
                if event['type'] == 'bar_batch':
                    d = event['data']
                    try:
                        self.assertLess(d['open'].max(), 68)
                        self.assertGreater(d['open'].min(), 65)
                    finally:
                        e1.set()

            listeners += process_bar

            listener.start()

            e1.wait()

            for i, d in enumerate(provider):
                self.assertLess(d['open'].max(), 68)
                self.assertGreater(d['open'].min(), 65)

                if i == 1:
                    break

    def test_bar_split_adjust_2(self):
        filter_provider = DefaultFilterProvider()
        filter_provider += BarsInPeriodFilter(ticker=["PLUS", "AAPL"], bgn_prd=datetime.datetime(2017, 3, 31), end_prd=datetime.datetime(2017, 4, 5), interval_len=3600, ascend=True, interval_type='s')

        listeners = AsyncListeners()

        with IQFeedHistoryEvents(listeners=listeners, fire_batches=True, filter_provider=filter_provider, sync_timestamps=False, timestamp_first=True, num_connections=2) as listener, listener.batch_provider() as provider:
            listener.start()

            for i, d in enumerate(provider):
                idx = pd.IndexSlice

                self.assertLess(d.loc[idx[:, 'PLUS'], 'open'].max(), 68)
                self.assertGreater(d.loc[idx[:, 'PLUS'], 'open'].min(), 65)
                self.assertGreater(d.loc[idx[:, 'AAPL'], 'open'].min(), 142)

                if i == 1:
                    break

    def test_exclude_splits(self):
        with IQFeedHistoryProvider() as provider:
            # single index
            f = BarsInPeriodFilter(ticker="PLUS", bgn_prd=datetime.datetime(2017, 3, 31), end_prd=datetime.datetime(2017, 4, 5), interval_len=3600, ascend=True, interval_type='s', max_ticks=100)

            data = provider.request_data(f, sync_timestamps=False)
            data['include'] = True
            data = data['include'].copy()

            conn = iq.QuoteConn()
            conn.connect()
            try:
                sd = get_splits_dividends(f.ticker, conn=conn)
            finally:
                conn.disconnect()

            result = exclude_splits(data, sd['value'].xs('split', level='type'), 10)

            self.assertTrue(result[~result].size == 10)

            # multiindex
            f = BarsInPeriodFilter(ticker=["PLUS", "IBM"], bgn_prd=datetime.datetime(2017, 3, 31), end_prd=datetime.datetime(2017, 4, 5), interval_len=3600, ascend=True, interval_type='s', max_ticks=100)

            data = provider.request_data(f, sync_timestamps=False)
            data['include'] = True
            data = data['include'].copy()

            conn = iq.QuoteConn()
            conn.connect()
            try:
                sd = get_splits_dividends(f.ticker, conn=conn)
            finally:
                conn.disconnect()

            result = exclude_splits(data, sd['value'].xs('split', level='type'), 10)

            self.assertTrue(result[~result].size == 10)

    def test_exclude_splits_performance(self):
        logging.basicConfig(level=logging.DEBUG)

        batch_len = 15000
        batch_width = 4000

        now = datetime.datetime.now()
        with IQFeedHistoryProvider() as provider:
            df1 = provider.request_data(BarsFilter(ticker="PLUS", interval_len=3600, interval_type='s', max_bars=batch_len), sync_timestamps=False)

            df = {'PLUS': df1}
            for i in range(batch_width):
                df['PLUS_' + str(i)] = df1.sample(random.randint(int(len(df1) / 3), len(df1) - 1))

            df = pd.concat(df, sort=True)
            df.index.set_names(['symbol', 'timestamp'], inplace=True)
            df['include'] = True
            data = df['include']

            conn = iq.QuoteConn()
            conn.connect()
            try:
                sd = get_splits_dividends("PLUS", conn=conn).xs('split', level='type')
            finally:
                conn.disconnect()

            splits = list()
            for l in df.index.levels[0]:
                ind_cp = sd.index.set_levels([l], level=1)
                for i, v in enumerate(sd):
                    ind_cp.values[i] = (sd.index.values[i][0], l, sd.index.values[i][2])

                cp = pd.DataFrame(data=sd.values, index=ind_cp)

                splits.append(cp)

            splits = pd.concat(splits, sort=True)

            logging.getLogger(__name__).debug('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(df.shape))

            now = datetime.datetime.now()

            result = exclude_splits(data, splits, 10)

            logging.getLogger(__name__).debug('Task done in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(result.shape))

            self.assertTrue(result[~result].size > 10)
            self.assertTrue(result[result].size > 0)
