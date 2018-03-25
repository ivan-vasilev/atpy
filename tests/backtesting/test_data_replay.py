import random
import unittest

from atpy.backtesting.data_replay import DataReplay, DataReplayEvents
from atpy.data.iqfeed.iqfeed_history_provider import *
from atpy.data.latest_data_snapshot import LatestDataSnapshot
from atpy.data.ts_util import current_period, AsyncInPeriodProvider
from pyevents.events import SyncListeners


class TestDataReplay(unittest.TestCase):
    """
    Test Data Replay
    """

    def test_basic(self):
        batch_len = 1000

        with IQFeedHistoryProvider() as provider:
            q = queue.Queue()
            provider.request_data_by_filters([BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=batch_len),
                                              BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len)],
                                             q)

            timestamps = set()

            dr = DataReplay().add_source(AsyncInPeriodProvider([q.get()[1]]), 'e1').add_source([q.get()[1]], 'e2')

            for i, r in enumerate(dr):
                for e in r:
                    t = r[e].iloc[0]['timestamp']

                if len(timestamps) > 0:
                    self.assertGreater(t, max(timestamps))

                timestamps.add(t)

                self.assertTrue(isinstance(r, dict))
                self.assertGreaterEqual(len(r), 1)

            self.assertGreaterEqual(len(timestamps), batch_len)

    def test_events(self):
        batch_len = 1000

        with IQFeedHistoryProvider() as provider:
            q = queue.Queue()
            provider.request_data_by_filters([BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=batch_len),
                                              BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len)],
                                             q)

            listeners = SyncListeners()

            timestamps = set()

            def check_df(event):
                if event is not None and event['type'] == 'data':
                    for e in event:
                        if isinstance(event[e], pd.DataFrame):
                            t = event[e].iloc[0]['timestamp']

                    if len(timestamps) > 0:
                        self.assertGreater(t, max(timestamps))

                    timestamps.add(t)

                    self.assertTrue(isinstance(event, dict))
                    self.assertGreaterEqual(len(event), 2)

            listeners += check_df

            data_replay = DataReplay().add_source([q.get()[1]], 'e1').add_source([q.get()[1]], 'e2')
            DataReplayEvents(listeners=listeners, data_replay=data_replay, event_name='data').start()

            self.assertGreaterEqual(len(timestamps), batch_len)

    def test_2(self):
        historical_depth = 10
        with IQFeedHistoryProvider(num_connections=1) as provider:
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

            l1 = [q1.get()[1], q1.get()[1], q1.get()[1], q1.get()[1]]
            l2 = [q2.get()[1], q2.get()[1], q2.get()[1]]

            maxl = max(max([len(l) for l in l1]), max([len(l) for l in l2]))
            timestamps = set()
            counters = {'e1': 0, 'e2': 0}

            dr = DataReplay().add_source(AsyncInPeriodProvider(l1), 'e1', historical_depth=historical_depth).add_source(l2, 'e2', historical_depth=historical_depth)

            for r in dr:
                for e in r:
                    t = r[e].iloc[-1]['timestamp']

                if len(timestamps) > 0:
                    self.assertGreater(t, max(timestamps))

                for e, df in r.items():
                    self.assertTrue(df.index.is_monotonic)

                    counters[e] = 1 if e not in counters else counters[e] + 1
                    self.assertEqual(df.shape[0], min(counters[e], historical_depth + 1))

                timestamps.add(t)

                self.assertTrue(isinstance(r, dict))
                self.assertGreaterEqual(len(r), 1)

            self.assertGreater(maxl, 0)
            self.assertGreaterEqual(len(timestamps), maxl)

            months = set()
            for t in timestamps:
                months.add(t.month)

            self.assertTrue({3, 4, 5, 6, 8} < months)

    def test_3_performance(self):
        logging.basicConfig(level=logging.DEBUG)

        batch_len = 10000
        batch_width = 500

        with IQFeedHistoryProvider() as provider:
            now = datetime.datetime.now()

            q = queue.Queue()
            provider.request_data_by_filters([BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len),
                                              BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=batch_len)],
                                             q)

            df1 = q.get()[1]
            dfs1 = {'AAPL': df1}
            for i in range(batch_width):
                dfs1['AAPL_' + str(i)] = df1.sample(random.randint(int(len(df1) / 3), len(df1) - 1))

            dfs1 = pd.concat(dfs1).swaplevel(0, 1)
            dfs1.sort_index(inplace=True)

            logging.getLogger(__name__).debug('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(dfs1.shape))

            now = datetime.datetime.now()

            dr = DataReplay().add_source([dfs1], 'e1', historical_depth=100)

            for i, r in enumerate(dr):
                if i % 1000 == 0 and i > 0:
                    new_now = datetime.datetime.now()
                    elapsed = new_now - now
                    logging.getLogger(__name__).debug('Time elapsed ' + str(elapsed) + ' for ' + str(i) + ' iterations; ' + str(elapsed / 1000) + ' per iteration')
                    now = new_now

                for e in r:
                    current_period(r[e])

            elapsed = datetime.datetime.now() - now
            logging.getLogger(__name__).debug('Time elapsed ' + str(elapsed) + ' for ' + str(i + 1) + ' iterations; ' + str(elapsed / (i % 1000)) + ' per iteration')

    def test_4_performance(self):
        logging.basicConfig(level=logging.DEBUG)

        batch_len = 10000
        batch_width = 5000

        with IQFeedHistoryProvider() as provider:
            now = datetime.datetime.now()

            q = queue.Queue()
            provider.request_data_by_filters([BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len),
                                              BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=batch_len)],
                                             q)

            df1 = q.get()[1]
            dfs1 = {'AAPL': df1}
            for i in range(batch_width):
                dfs1['AAPL_' + str(i)] = df1.sample(random.randint(int(len(df1) / 3), len(df1) - 1))

            dfs1 = pd.concat(dfs1).swaplevel(0, 1)
            dfs1.sort_index(inplace=True)

            df2 = q.get()[1]
            dfs2 = {'IBM': df2}
            for i in range(batch_width):
                dfs2['IBM_' + str(i)] = df2.sample(random.randint(int(len(df2) / 3), len(df2) - 1))

            dfs2 = pd.concat(dfs2).swaplevel(0, 1)
            dfs2.sort_index(inplace=True)

            logging.getLogger(__name__).debug('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(dfs1.shape) + ', ' + str(dfs2.shape))

            dr = DataReplay().add_source([dfs1], 'e1', historical_depth=100).add_source([dfs2], 'e2', historical_depth=100)

            now = datetime.datetime.now()

            for i, r in enumerate(dr):
                if i % 1000 == 0 and i > 0:
                    new_now = datetime.datetime.now()
                    elapsed = new_now - now
                    logging.getLogger(__name__).debug('Time elapsed ' + str(elapsed) + ' for ' + str(i) + ' iterations; ' + str(elapsed / 1000) + ' per iteration')
                    now = new_now

                for e in r:
                    current_period(r[e])

            elapsed = datetime.datetime.now() - now
            logging.getLogger(__name__).debug('Time elapsed ' + str(elapsed) + ' for ' + str(i + 1) + ' iterations; ' + str(elapsed / (i % 1000)) + ' per iteration')

    def test_4_validity(self):
        logging.basicConfig(level=logging.DEBUG)

        batch_len = 10000
        batch_width = 500

        with IQFeedHistoryProvider() as provider:
            now = datetime.datetime.now()

            q = queue.Queue()
            provider.request_data_by_filters([BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len),
                                              BarsFilter(ticker="IBM", interval_len=60, interval_type='s', max_bars=batch_len)],
                                             q)

            df1 = q.get()[1]
            dfs1 = {'AAPL': df1}
            for i in range(batch_width):
                dfs1['AAPL_' + str(i)] = df1.sample(random.randint(int(len(df1) / 3), len(df1) - 1))

            dfs1 = pd.concat(dfs1).swaplevel(0, 1)
            dfs1.sort_index(inplace=True)

            df2 = q.get()[1]
            dfs2 = {'IBM': df2}
            for i in range(batch_width):
                dfs2['IBM_' + str(i)] = df2.sample(random.randint(int(len(df2) / 3), len(df2) - 1))

            dfs2 = pd.concat(dfs2).swaplevel(0, 1)
            dfs2.sort_index(inplace=True)

            logging.getLogger(__name__).debug('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shapes ' + str(dfs1.shape) + ', ' + str(dfs2.shape))

            dr = DataReplay().add_source([dfs1], 'e1', historical_depth=100).add_source([dfs2], 'e2', historical_depth=100)
            prev_t = None
            now = datetime.datetime.now()

            for i, r in enumerate(dr):
                if i % 1000 == 0 and i > 0:
                    new_now = datetime.datetime.now()
                    elapsed = new_now - now
                    logging.getLogger(__name__).debug('Time elapsed ' + str(elapsed) + ' for ' + str(i) + ' iterations; ' + str(elapsed / 1000) + ' per iteration')
                    now = new_now

                for e in r:
                    x, a = current_period(r[e])
                    self.assertFalse(x.empty)
                    t = r[e].iloc[-1]['timestamp']

                if prev_t is not None:
                    self.assertGreater(t, prev_t)

                prev_t = t
                self.assertTrue(isinstance(r, dict))
                self.assertGreaterEqual(len(r), 1)

            elapsed = datetime.datetime.now() - now
            logging.getLogger(__name__).debug('Time elapsed ' + str(elapsed) + ' for ' + str(i + 1) + ' iterations; ' + str(elapsed / (i % 1000)) + ' per iteration')

            self.assertIsNotNone(t)
            self.assertIsNotNone(prev_t)

    def test_5(self):
        logging.basicConfig(level=logging.DEBUG)

        batch_len = 1000
        batch_width = 500

        with IQFeedHistoryProvider() as provider:
            now = datetime.datetime.now()

            q = queue.Queue()
            provider.request_data_by_filters([BarsFilter(ticker="AAPL", interval_len=60, interval_type='s', max_bars=batch_len)], q)

            df1 = q.get()[1]
            dfs1 = {'AAPL': df1}
            for i in range(batch_width):
                dfs1['AAPL_' + str(i)] = df1.sample(random.randint(int(len(df1) / 3), len(df1) - 1))

            dfs1 = pd.concat(dfs1).swaplevel(0, 1).sort_index()

            dr = DataReplay().add_source([dfs1], 'e1', historical_depth=0)
            logging.getLogger(__name__).debug('Random data generated in ' + str(datetime.datetime.now() - now) + ' with shape ' + str(dfs1.shape))

            prev_t = None
            now = datetime.datetime.now()

            listeners = SyncListeners()
            lb = LatestDataSnapshot(listeners=listeners, event='event', fire_update=True, depth=100)

            j = 0

            snapshots_count = {'count': 0}

            def snapshot_listener(event):
                if event['type'] == 'event_snapshot':
                    self.assertEqual(len(event['data'].index.levels[0]), min(lb.depth, j + 1))
                    snapshots_count['count'] += 1

            listeners += snapshot_listener
            for i, r in enumerate(dr):
                j = i
                for a in r:
                    lb.on_event({'type': 'event', 'data': r[a]})

                if i % 100 == 0 and i > 0:
                    new_now = datetime.datetime.now()
                    elapsed = new_now - now
                    logging.getLogger(__name__).debug('Time elapsed ' + str(elapsed) + ' for ' + str(i) + ' iterations; ' + str(elapsed / 100) + ' per iteration')
                    now = new_now

                for e in r:
                    t = r[e].iloc[-1]['timestamp']

                if prev_t is not None:
                    self.assertGreater(t, prev_t)

                prev_t = t
                self.assertTrue(isinstance(r, dict))
                self.assertGreaterEqual(len(r), 1)

            elapsed = datetime.datetime.now() - now
            logging.getLogger(__name__).debug('Time elapsed ' + str(elapsed) + ' for ' + str(i + 1) + ' iterations; ' + str(elapsed / (i % 100)) + ' per iteration')

            self.assertIsNotNone(t)
            self.assertIsNotNone(prev_t)
            self.assertEqual(batch_len, snapshots_count['count'])


if __name__ == '__main__':
    unittest.main()
