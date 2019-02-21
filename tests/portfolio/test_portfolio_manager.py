import unittest

from atpy.backtesting.data_replay import DataReplayEvents, DataReplay
from atpy.backtesting.mock_exchange import MockExchange, PerShareCommissionLoss
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

        pm = PortfolioManager(listeners=listeners, initial_capital=10000)

        # order 1
        o2 = MarketOrder(Type.BUY, 'GOOG', 100)
        o2.add_position(14, 23)
        o2.add_position(86, 24)
        o2.fulfill_time = datetime.datetime.now()

        e1 = threading.Event()
        pm.listeners += lambda x: e1.set()
        pm.listeners({'type': 'order_fulfilled', 'data': o2})
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
        pm.listeners({'type': 'order_fulfilled', 'data': o2})
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
        pm.listeners({'type': 'order_fulfilled', 'data': o3})
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
        pm.listeners({'type': 'order_fulfilled', 'data': o4})
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

            pm = PortfolioManager(listeners=listeners, initial_capital=10000)

            store = MongoDBStore(client.test_db.store, lambda event: event['type'] == 'portfolio_update', listeners=listeners)

            # order 1
            o1 = MarketOrder(Type.BUY, 'GOOG', 100)
            o1.add_position(14, 23)
            o1.add_position(86, 24)
            o1.fulfill_time = datetime.datetime.now()

            e1 = threading.Event()
            listeners += lambda x: e1.set() if x['type'] == 'store_object' else None
            listeners({'type': 'order_fulfilled', 'data': o1})
            e1.wait()

            # order 2
            o2 = MarketOrder(Type.BUY, 'AAPL', 50)
            o2.add_position(50, 21)
            o2.fulfill_time = datetime.datetime.now()

            e2 = threading.Event()
            listeners += lambda x: e2.set() if x['type'] == 'store_object' else None
            listeners({'type': 'order_fulfilled', 'data': o2})
            e2.wait()

            obj = store.restore(client.test_db.store, pm._id)
            self.assertEqual(obj._id, pm._id)
            self.assertEqual(len(obj.orders), 2)
            self.assertEqual(obj.initial_capital, 10000)
        finally:
            client.drop_database('test_db')

    def test_price_updates(self):
        listeners = AsyncListeners()

        with IQFeedLevel1Listener(listeners=listeners, minibatch=2):
            pm = PortfolioManager(listeners=listeners, initial_capital=10000)

            # order 1
            o1 = MarketOrder(Type.BUY, 'GOOG', 100)
            o1.add_position(14, 1)
            o1.add_position(86, 1)
            o1.fulfill_time = datetime.datetime.now()

            e1 = threading.Event()
            listeners += lambda x: e1.set() if x['type'] == 'portfolio_value_update' else None
            listeners({'type': 'order_fulfilled', 'data': o1})
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
            listeners += lambda x: e3.set() if x['type'] == 'portfolio_value_update' else None
            listeners({'type': 'order_fulfilled', 'data': o3})
            e3.wait()

            self.assertNotEqual(pm.value('GOOG'), 1)
            self.assertNotEqual(pm.value('GOOG'), 0.5)
            self.assertNotEqual(pm.value('AAPL'), 0.2)
            self.assertEqual(len(pm._values), 2)

    def test_historical_price_updates(self):
        listeners = AsyncListeners()

        pm = PortfolioManager(listeners=listeners, initial_capital=10000)

        # order 1
        o1 = MarketOrder(Type.BUY, 'GOOG', 100)
        o1.add_position(14, 1)
        o1.add_position(86, 1)
        o1.fulfill_time = datetime.datetime.now()

        e1 = threading.Event()
        listeners += lambda x: e1.set() if x['type'] == 'portfolio_value_update' else None
        listeners({'type': 'order_fulfilled', 'data': o1})

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
        listeners({'type': 'order_fulfilled', 'data': o3})

        # historical data
        with IQFeedHistoryProvider() as provider:
            f = BarsFilter(ticker=["GOOG", "AAPL"], interval_len=60, interval_type='s', max_bars=5)
            data = provider.request_data(f, sync_timestamps=False).swaplevel(0, 1).sort_index()
            DataReplayEvents(listeners, DataReplay().add_source([data], 'data', historical_depth=2), event_name='bar').start()

            e1.wait()
            e3.wait()

        self.assertNotEqual(pm.value('GOOG'), 1)
        self.assertNotEqual(pm.value('GOOG'), 0.5)
        self.assertNotEqual(pm.value('AAPL'), 0.2)
        self.assertEqual(len(pm._values), 2)

    def test_mock_orders(self):
        listeners = AsyncListeners()

        with IQFeedLevel1Listener(listeners=listeners) as level1:
            pm = PortfolioManager(listeners=listeners, initial_capital=10000)

            MockExchange(listeners=listeners, commission_loss=PerShareCommissionLoss(0.1))

            e1 = threading.Event()
            listeners += lambda x: e1.set() if x['type'] == 'portfolio_update' and 'GOOG' in x['data'].symbols else None

            e2 = threading.Event()
            listeners += lambda x: e2.set() if x['type'] == 'portfolio_update' and 'AAPL' in x['data'].symbols else None

            e3 = threading.Event()
            listeners += lambda x: e3.set() if x['type'] == 'portfolio_update' and 'IBM' in x['data'].symbols else None

            o1 = StopLimitOrder(Type.BUY, 'GOOG', 1, 99999, 1)
            listeners({'type': 'order_request', 'data': o1})

            o2 = StopLimitOrder(Type.BUY, 'AAPL', 3, 99999, 1)
            listeners({'type': 'order_request', 'data': o2})

            o3 = StopLimitOrder(Type.BUY, 'IBM', 1, 99999, 1)
            listeners({'type': 'order_request', 'data': o3})

            o4 = StopLimitOrder(Type.SELL, 'AAPL', 1, 1, 99999)
            listeners({'type': 'order_request', 'data': o4})

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
        listeners = AsyncListeners()

        pm = PortfolioManager(listeners=listeners, initial_capital=10000)

        MockExchange(listeners=listeners, commission_loss=PerShareCommissionLoss(0.1))

        e1 = threading.Event()
        listeners += lambda x: e1.set() if x['type'] == 'portfolio_update' and 'GOOG' in x['data'].symbols else None

        e2 = threading.Event()
        listeners += lambda x: e2.set() if x['type'] == 'portfolio_update' and 'AAPL' in x['data'].symbols else None

        e3 = threading.Event()
        listeners += lambda x: e3.set() if x['type'] == 'portfolio_update' and 'IBM' in x['data'].symbols else None

        o1 = StopLimitOrder(Type.BUY, 'GOOG', 1, 99999, 1)
        listeners({'type': 'order_request', 'data': o1})

        o2 = StopLimitOrder(Type.BUY, 'AAPL', 3, 99999, 1)
        listeners({'type': 'order_request', 'data': o2})

        o3 = StopLimitOrder(Type.BUY, 'IBM', 1, 99999, 1)
        listeners({'type': 'order_request', 'data': o3})

        o4 = StopLimitOrder(Type.SELL, 'AAPL', 1, 1, 99999)
        listeners({'type': 'order_request', 'data': o4})

        # historical data
        with IQFeedHistoryProvider() as provider:
            f = BarsFilter(ticker=["GOOG", "AAPL", "IBM"], interval_len=60, interval_type='s', max_bars=5)
            data = provider.request_data(f, sync_timestamps=False).swaplevel(0, 1).sort_index()
            DataReplayEvents(listeners, DataReplay().add_source([data], 'data', historical_depth=2), event_name='bar').start()

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
