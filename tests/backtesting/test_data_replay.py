import unittest

from atpy.backtesting.data_replay import DataReplay
from atpy.data.iqfeed.iqfeed_history_provider import *


class TestDataReplay(unittest.TestCase):
    """
    Test Data Replay
    """

    def test_basic(self):
        batch_len = 1000

        l1, l2 = list(), list()
        with IQFeedHistoryProvider() as provider, DataReplay().add_source(iter(l1), 'e1').add_source(iter(l2), 'e2') as dr:
            q = queue.Queue()
            provider.request_data_by_filters([BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=batch_len),
                                              BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len)],
                                             q)

            l1.append(q.get()[1])
            l2.append(q.get()[1])

            timestamps = set()
            for i, r in enumerate(dr):
                for e in r:
                    t = r[e]['timestamp']

                if len(timestamps) > 0:
                    self.assertGreater(t, max(timestamps))

                timestamps.add(t)

                self.assertTrue(isinstance(r, dict))
                self.assertGreaterEqual(len(r), 1)

            self.assertGreaterEqual(len(timestamps), batch_len)

    def test_2(self):
        l1, l2 = list(), list()
        with IQFeedHistoryProvider(num_connections=1) as provider, DataReplay().add_source(iter(l1), 'e1').add_source(iter(l2), 'e2') as dr:
            year = datetime.datetime.now().year - 1

            q1 = queue.Queue()
            provider.request_data_by_filters([BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(year, 3, 1), end_prd=datetime.datetime(year, 4, 1), interval_len=3600, ascend=True, interval_type='s'),
                                              BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(year, 4, 2), end_prd=datetime.datetime(year, 5, 1), interval_len=3600, ascend=True, interval_type='s'),
                                              BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(year, 5, 2), end_prd=datetime.datetime(year, 6, 1), interval_len=3600, ascend=True, interval_type='s'),
                                              BarsInPeriodFilter(ticker="AAPL", bgn_prd=datetime.datetime(year, 8, 2), end_prd=datetime.datetime(year, 9, 1), interval_len=3600, ascend=True, interval_type='s')],
                                             q1)

            q2 = queue.Queue()
            provider.request_data_by_filters([BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(year, 4, 1), end_prd=datetime.datetime(year, 5, 1), interval_len=3600, ascend=True, interval_type='s'),
                                              BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(year, 5, 2), end_prd=datetime.datetime(year, 6, 1), interval_len=3600, ascend=True, interval_type='s'),
                                              BarsInPeriodFilter(ticker="IBM", bgn_prd=datetime.datetime(year, 6, 2), end_prd=datetime.datetime(year, 7, 1), interval_len=3600, ascend=True, interval_type='s')],
                                             q2)

            l1.append(q1.get()[1])
            l1.append(q1.get()[1])
            l1.append(q1.get()[1])
            l1.append(q1.get()[1])

            l2.append(q2.get()[1])
            l2.append(q2.get()[1])
            l2.append(q2.get()[1])

            maxl = max(max([len(l) for l in l1]), max([len(l) for l in l2]))
            timestamps = set()
            for i, r in enumerate(dr):
                for e in r:
                    t = r[e]['timestamp']

                if len(timestamps) > 0:
                    self.assertGreater(t, max(timestamps))

                timestamps.add(t)

                self.assertTrue(isinstance(r, dict))
                self.assertGreaterEqual(len(r), 1)

            self.assertGreater(maxl, 0)
            self.assertGreaterEqual(len(timestamps), maxl)

            months = set()
            for t in timestamps:
                months.add(t.month)

            self.assertTrue({3, 4, 5, 6, 8} < months)


if __name__ == '__main__':
    unittest.main()
