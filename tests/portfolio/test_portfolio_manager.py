import unittest

from atpy.backtesting.data_replay import DataReplayEvents, DataReplay
from atpy.backtesting.mock_exchange import MockExchange, PerShareCommissionLoss, StaticSlippageLoss
from atpy.data.iqfeed.iqfeed_history_provider import *
from atpy.data.iqfeed.iqfeed_level_1_provider import *
from atpy.portfolio.portfolio_manager import *
from pyevents.events import AsyncListeners
from pyevents_util.mongodb.mongodb_store import *


class TestPortfolioManager(unittest.TestCase):
    """
    Test portfolio manager
    """

    def test_1(self):
        listeners = AsyncListeners()
        fulfilled_orders = SyncListeners()

        pm = PortfolioManager(listeners=listeners,
                              initial_capital=10000,
                              fulfilled_orders_event_stream=fulfilled_orders)

        # order 1
        o1 = MarketOrder(Type.BUY, 'GOOG', 100)
        o1.add_position(14, 23)
        o1.add_position(86, 24)
        o1.fulfill_time = datetime.datetime.now()

        e1 = threading.Event()
        listeners += lambda x: e1.set()
        fulfilled_orders(o1)
        e1.wait()

        self.assertEqual(len(pm.quantity()), 1)
        self.assertEqual(pm.quantity()['GOOG'], 100)
        self.assertEqual(pm.quantity('GOOG'), 100)
        self.assertEqual(pm.value('GOOG', multiply_by_quantity=True), 100 * 24)
        self.assertEqual(len(pm.value()), 1)
        self.assertEqual(pm.value(multiply_by_quantity=True)['GOOG'], 100 * 24)
        self.assertEqual(pm.capital, 10000 - (14 * 23 + 86 * 24))

        # order 2
        o2 = MarketOrder(Type.BUY, 'GOOG', 150)
        o2.add_position(110, 25)
        o2.add_position(30, 26)
        o2.add_position(10, 27)
        o2.fulfill_time = datetime.datetime.now()

        e2 = threading.Event()
        pm.listeners += lambda x: e2.set()
        fulfilled_orders(o2)
        e2.wait()

        self.assertEqual(len(pm.quantity()), 1)
        self.assertEqual(pm.quantity()['GOOG'], 250)
        self.assertEqual(pm.quantity('GOOG'), 250)
        self.assertEqual(pm.value('GOOG', multiply_by_quantity=True), 250 * 27)
        self.assertEqual(len(pm.value()), 1)
        self.assertEqual(pm.value(multiply_by_quantity=True)['GOOG'], 250 * 27)
        self.assertEqual(pm.capital, 10000 - (14 * 23 + 86 * 24 + 110 * 25 + 30 * 26 + 10 * 27))

        # order 3
        o3 = MarketOrder(Type.SELL, 'GOOG', 60)
        o3.add_position(60, 22)
        o3.fulfill_time = datetime.datetime.now()

        e3 = threading.Event()
        pm.listeners += lambda x: e3.set()
        fulfilled_orders(o3)
        e3.wait()

        self.assertEqual(len(pm.quantity()), 1)
        self.assertEqual(pm.quantity()['GOOG'], 190)
        self.assertEqual(pm.quantity('GOOG'), 190)
        self.assertEqual(pm.value('GOOG'), 22)
        self.assertEqual(len(pm.value()), 1)
        self.assertEqual(pm.value()['GOOG'], 22)
        self.assertEqual(pm.capital, 10000 - (14 * 23 + 86 * 24 + 110 * 25 + 30 * 26 + 10 * 27) + 60 * 22)

        # order 4
        o4 = MarketOrder(Type.BUY, 'AAPL', 50)
        o4.add_position(50, 21)
        o4.fulfill_time = datetime.datetime.now()

        e4 = threading.Event()
        pm.listeners += lambda x: e4.set()
        fulfilled_orders(o4)
        e4.wait()

        self.assertEqual(len(pm.quantity()), 2)
        self.assertEqual(pm.quantity()['AAPL'], 50)
        self.assertEqual(pm.quantity('AAPL'), 50)
        self.assertEqual(pm.value('AAPL', multiply_by_quantity=True), 50 * 21)
        self.assertEqual(len(pm.value()), 2)
        self.assertEqual(pm.value(multiply_by_quantity=True)['AAPL'], 50 * 21)
        self.assertEqual(pm.capital, 10000 - (14 * 23 + 86 * 24 + 110 * 25 + 30 * 26 + 10 * 27 + 50 * 21) + 60 * 22)

    def test_logging(self):
        client = pymongo.MongoClient()

        try:
            # logging.basicConfig(level=logging.DEBUG)
            listeners = AsyncListeners()

            fulfilled_orders = SyncListeners()

            pm = PortfolioManager(listeners=listeners,
                                  initial_capital=10000,
                                  fulfilled_orders_event_stream=fulfilled_orders)

            store = MongoDBStore(client.test_db.store, lambda event: event['type'] == 'portfolio_update', listeners=listeners)

            # order 1
            o1 = MarketOrder(Type.BUY, 'GOOG', 100)
            o1.add_position(14, 23)
            o1.add_position(86, 24)
            o1.fulfill_time = datetime.datetime.now()

            e1 = threading.Event()
            listeners += lambda x: e1.set() if x['type'] == 'store_object' else None
            fulfilled_orders(o1)
            e1.wait()

            # order 2
            o2 = MarketOrder(Type.BUY, 'AAPL', 50)
            o2.add_position(50, 21)
            o2.fulfill_time = datetime.datetime.now()

            e2 = threading.Event()
            listeners += lambda x: e2.set() if x['type'] == 'store_object' else None
            fulfilled_orders(o2)
            e2.wait()

            obj = store.restore(client.test_db.store, pm._id)
            self.assertEqual(obj._id, pm._id)
            self.assertEqual(len(obj.orders), 2)
            self.assertEqual(obj.initial_capital, 10000)
        finally:
            client.drop_database('test_db')

    def test_price_updates(self):
        listeners = AsyncListeners()

        with IQFeedLevel1Listener(listeners=listeners, minibatch=2) as level_1:
            listeners = AsyncListeners()

            fulfilled_orders = SyncListeners()

            pm = PortfolioManager(listeners=listeners,
                                  initial_capital=10000,
                                  tick_event_stream=level_1.tick_event_filter(),
                                  fulfilled_orders_event_stream=fulfilled_orders)

            # order 1
            o1 = MarketOrder(Type.BUY, 'GOOG', 100)
            o1.add_position(14, 1)
            o1.add_position(86, 1)
            o1.fulfill_time = datetime.datetime.now()

            e1 = threading.Event()
            listeners += lambda x: e1.set() if x['type'] == 'portfolio_value_update' and pm.value('GOOG') > 1 else None
            fulfilled_orders(o1)

            level_1.watch('GOOG')

            e1.wait()

            self.assertNotEqual(pm.value('GOOG'), 1)

            # order 2
            o2 = MarketOrder(Type.BUY, 'GOOG', 90)
            o2.add_position(4, 0.5)
            o2.add_position(86, 0.5)
            o2.fulfill_time = datetime.datetime.now()

            self.assertNotEqual(pm.value('GOOG'), 1)
            self.assertNotEqual(pm.value('GOOG'), 0.5)

            # order 3
            o3 = MarketOrder(Type.BUY, 'AAPL', 100)
            o3.add_position(14, 0.2)
            o3.add_position(86, 0.2)
            o3.fulfill_time = datetime.datetime.now()

            e3 = threading.Event()
            listeners += lambda x: e3.set() if x['type'] == 'portfolio_value_update' and pm.value('AAPL') > 0.2 else None
            fulfilled_orders(o3)

            level_1.watch('AAPL')

            e3.wait()

            self.assertNotEqual(pm.value('GOOG'), 1)
            self.assertNotEqual(pm.value('GOOG'), 0.5)
            self.assertNotEqual(pm.value('AAPL'), 0.2)
            self.assertEqual(len(pm._values), 2)

    def test_historical_price_updates(self):
        listeners = AsyncListeners()
        fulfilled_orders = SyncListeners()

        # order 1
        o1 = MarketOrder(Type.BUY, 'GOOG', 100)
        o1.add_position(14, 1)
        o1.add_position(86, 1)
        o1.fulfill_time = datetime.datetime.now()

        e1 = threading.Event()
        listeners += lambda x: e1.set() if x['type'] == 'portfolio_value_update' else None

        # order 2
        o2 = MarketOrder(Type.BUY, 'GOOG', 90)
        o2.add_position(4, 0.5)
        o2.add_position(86, 0.5)
        o2.fulfill_time = datetime.datetime.now()

        # order 3
        o3 = MarketOrder(Type.BUY, 'AAPL', 100)
        o3.add_position(14, 0.2)
        o3.add_position(86, 0.2)
        o3.fulfill_time = datetime.datetime.now()

        e3 = threading.Event()
        listeners += lambda x: e3.set() if x['type'] == 'portfolio_value_update' else None

        # historical data
        with IQFeedHistoryProvider() as provider:
            f = BarsFilter(ticker=["GOOG", "AAPL"], interval_len=60, interval_type='s', max_bars=5)
            data = provider.request_data(f, sync_timestamps=False).swaplevel(0, 1).sort_index()
            dre = DataReplayEvents(listeners, DataReplay().add_source([data], 'data', historical_depth=2), event_name='bar')

            pm = PortfolioManager(listeners=listeners,
                                  initial_capital=10000,
                                  fulfilled_orders_event_stream=fulfilled_orders,
                                  bar_event_stream=dre.event_filter_by_source('data'))

            fulfilled_orders(o1)
            fulfilled_orders(o3)

            dre.start()

            e1.wait()
            e3.wait()

        self.assertNotEqual(pm.value('GOOG'), 1)
        self.assertNotEqual(pm.value('GOOG'), 0.5)
        self.assertNotEqual(pm.value('AAPL'), 0.2)
        self.assertEqual(len(pm._values), 2)

    def test_mock_orders(self):
        listeners = SyncListeners()

        with IQFeedLevel1Listener(listeners=listeners) as level1:
            order_request_events = SyncListeners()

            me = MockExchange(listeners=listeners,
                              order_requests_event_stream=order_request_events,
                              tick_event_stream=level1.tick_event_filter(),
                              order_processor=StaticSlippageLoss(0.1),
                              commission_loss=PerShareCommissionLoss(0.1))

            pm = PortfolioManager(listeners=listeners,
                                  initial_capital=10000,
                                  tick_event_stream=level1.tick_event_filter(),
                                  fulfilled_orders_event_stream=me.fulfilled_orders_stream())

            e1 = threading.Event()

            portfolio_updates = pm.portfolio_updates_stream()
            portfolio_updates += lambda x: e1.set() if 'GOOG' in x.symbols else None

            e2 = threading.Event()
            portfolio_updates += lambda x: e2.set() if 'AAPL' in x.symbols else None

            e3 = threading.Event()
            portfolio_updates += lambda x: e3.set() if 'IBM' in x.symbols else None

            o1 = StopLimitOrder(Type.BUY, 'GOOG', 1, 99999, 1)
            order_request_events(o1)

            o2 = StopLimitOrder(Type.BUY, 'AAPL', 3, 99999, 1)
            order_request_events(o2)

            o3 = StopLimitOrder(Type.BUY, 'IBM', 1, 99999, 1)
            order_request_events(o3)

            o4 = StopLimitOrder(Type.SELL, 'AAPL', 1, 1, 99999)
            order_request_events(o4)

            level1.watch('GOOG')
            level1.watch('AAPL')
            level1.watch('IBM')

            e1.wait()
            e2.wait()
            e3.wait()

        self.assertLess(pm.capital, pm.initial_capital)
        self.assertTrue('GOOG' in pm.symbols)
        self.assertTrue('AAPL' in pm.symbols)
        self.assertTrue('IBM' in pm.symbols)

        self.assertEqual(pm.quantity('GOOG'), 1)
        self.assertEqual(pm.quantity('AAPL'), 2)
        self.assertEqual(pm.quantity('IBM'), 1)

        self.assertGreater(pm.value('GOOG'), 0)
        self.assertGreater(pm.value('AAPL'), 0)
        self.assertGreater(pm.value('IBM'), 0)

    def test_historical_bar_mock_orders(self):
        with IQFeedHistoryProvider() as provider:
            listeners = AsyncListeners()

            f = BarsFilter(ticker=["GOOG", "AAPL", "IBM"], interval_len=60, interval_type='s', max_bars=5)
            data = provider.request_data(f, sync_timestamps=False).swaplevel(0, 1).sort_index()
            dre = DataReplayEvents(listeners=listeners,
                                   data_replay=DataReplay().add_source([data], 'data', historical_depth=5),
                                   event_name='bar')

            bars = dre.event_filter_by_source('data')

            order_request_events = SyncListeners()

            me = MockExchange(listeners=listeners,
                              order_requests_event_stream=order_request_events,
                              bar_event_stream=bars,
                              order_processor=StaticSlippageLoss(0.1),
                              commission_loss=PerShareCommissionLoss(0.1))

            pm = PortfolioManager(listeners=listeners,
                                  initial_capital=10000,
                                  bar_event_stream=bars,
                                  fulfilled_orders_event_stream=me.fulfilled_orders_stream())

            portfolio_updates = pm.portfolio_updates_stream()

            o1 = StopLimitOrder(Type.BUY, 'GOOG', 1, 99999, 1)
            o2 = StopLimitOrder(Type.BUY, 'AAPL', 3, 99999, 1)
            o3 = StopLimitOrder(Type.BUY, 'IBM', 1, 99999, 1)
            o4 = StopLimitOrder(Type.SELL, 'AAPL', 1, 1, 99999)

            order_request_events(o1)
            order_request_events(o2)
            order_request_events(o3)
            order_request_events(o4)

            e1 = threading.Event()
            portfolio_updates += lambda x: e1.set() if 'GOOG' in x.symbols else None

            e2 = threading.Event()
            portfolio_updates += lambda x: e2.set() if 'AAPL' in x.symbols else None

            e3 = threading.Event()
            portfolio_updates += lambda x: e3.set() if 'IBM' in x.symbols else None

            dre.start()

            e1.wait()
            e2.wait()
            e3.wait()

        self.assertLess(pm.capital, pm.initial_capital)
        self.assertTrue('GOOG' in pm.symbols)
        self.assertTrue('AAPL' in pm.symbols)
        self.assertTrue('IBM' in pm.symbols)

        self.assertEqual(pm.quantity('GOOG'), 1)
        self.assertEqual(pm.quantity('AAPL'), 2)
        self.assertEqual(pm.quantity('IBM'), 1)

        self.assertGreater(pm.value('GOOG'), 0)
        self.assertGreater(pm.value('AAPL'), 0)
        self.assertGreater(pm.value('IBM'), 0)


if __name__ == '__main__':
    unittest.main()
