import unittest
from atpy.portfolio.portfolio_manager import *
from pyevents_util.mongodb.mongodb_store import *
from atpy.data.iqfeed.iqfeed_level_1_provider import *


class TestPortfolioManager(unittest.TestCase):
    """
    Test portfolio manager
    """

    def test_1(self):
        pm = PortfolioManager(10000)

        # order 1
        o1 = MarketOrder(Type.BUY, 'GOOG', 100)
        o1.fulfill_time = datetime.datetime.now()
        o1.obtained_positions.append((14, 23))
        o1.obtained_positions.append((86, 24))

        pm.on_event({'type': 'order_fulfilled', 'order': o1})

        self.assertEqual(len(pm.quantity()), 1)
        self.assertEqual(pm.quantity()['GOOG'], 100)
        self.assertEqual(pm.quantity('GOOG'), 100)
        self.assertEqual(pm.value('GOOG', multiply_by_quantity=True), 100 * 24)
        self.assertEqual(len(pm.value()), 1)
        self.assertEqual(pm.value(multiply_by_quantity=True)['GOOG'], 100 * 24)
        self.assertEqual(pm.capital, 10000 - (14 * 23 + 86 * 24))

        # order 2
        o1 = MarketOrder(Type.BUY, 'GOOG', 150)
        o1.fulfill_time = datetime.datetime.now()
        o1.obtained_positions.append((110, 25))
        o1.obtained_positions.append((30, 26))
        o1.obtained_positions.append((10, 27))

        pm.on_event({'type': 'order_fulfilled', 'order': o1})

        self.assertEqual(len(pm.quantity()), 1)
        self.assertEqual(pm.quantity()['GOOG'], 250)
        self.assertEqual(pm.quantity('GOOG'), 250)
        self.assertEqual(pm.value('GOOG', multiply_by_quantity=True), 250 * 27)
        self.assertEqual(len(pm.value()), 1)
        self.assertEqual(pm.value(multiply_by_quantity=True)['GOOG'], 250 * 27)
        self.assertEqual(pm.capital, 10000 - (14 * 23 + 86 * 24 + 110 * 25 + 30 * 26 + 10 * 27))

        # order 3
        o1 = MarketOrder(Type.SELL, 'GOOG', 60)
        o1.fulfill_time = datetime.datetime.now()
        o1.obtained_positions.append((60, 22))

        pm.on_event({'type': 'order_fulfilled', 'order': o1})

        self.assertEqual(len(pm.quantity()), 1)
        self.assertEqual(pm.quantity()['GOOG'], 190)
        self.assertEqual(pm.quantity('GOOG'), 190)
        self.assertEqual(pm.value('GOOG'), 22)
        self.assertEqual(len(pm.value()), 1)
        self.assertEqual(pm.value()['GOOG'], 22)
        self.assertEqual(pm.capital, 10000 - (14 * 23 + 86 * 24 + 110 * 25 + 30 * 26 + 10 * 27) + 60 * 22)

        # order 4
        o1 = MarketOrder(Type.BUY, 'AAPL', 50)
        o1.fulfill_time = datetime.datetime.now()
        o1.obtained_positions.append((50, 21))

        pm.on_event({'type': 'order_fulfilled', 'order': o1})

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

            global_listeners = AsyncListeners()

            pm = PortfolioManager(10000, default_listeners=global_listeners)

            store = MongoDBStore(client.test_db.store, lambda event: event['type'] == 'portfolio_update', default_listeners=global_listeners)

            # order 1
            o1 = MarketOrder(Type.BUY, 'GOOG', 100)
            o1.fulfill_time = datetime.datetime.now()
            o1.obtained_positions.append((14, 23))
            o1.obtained_positions.append((86, 24))

            e1 = threading.Event()
            store.object_stored += lambda x: e1.set()
            pm.on_event({'type': 'order_fulfilled', 'order': o1})
            e1.wait()

            # order 2
            o2 = MarketOrder(Type.BUY, 'AAPL', 50)
            o2.fulfill_time = datetime.datetime.now()
            o2.obtained_positions.append((50, 21))

            e2 = threading.Event()
            store.object_stored += lambda x: e2.set()
            pm.on_event({'type': 'order_fulfilled', 'order': o2})
            e2.wait()

            obj = store.restore(client.test_db.store, pm._id)
            self.assertEqual(obj._id, pm._id)
            self.assertEqual(len(obj.orders), 2)
            self.assertEqual(obj.initial_capital, 10000)
        finally:
            client.drop_database('test_db')

    def test_price_updates(self):
        listeners = AsyncListeners()

        with IQFeedLevel1Listener(minibatch=2, default_listeners=listeners) as listener:
            pm = PortfolioManager(10000, default_listeners=listeners)

            # order 1
            o1 = MarketOrder(Type.BUY, 'GOOG', 100)
            o1.fulfill_time = datetime.datetime.now()
            o1.obtained_positions.append((14, 1))
            o1.obtained_positions.append((86, 1))

            e1 = threading.Event()
            pm.portfolio_value_update += lambda x: e1.set()
            pm.on_event({'type': 'order_fulfilled', 'order': o1})
            e1.wait()

            self.assertNotEquals(pm.value('GOOG'), 1)

            # order 2
            o2 = MarketOrder(Type.BUY, 'GOOG', 90)
            o2.fulfill_time = datetime.datetime.now()
            o2.obtained_positions.append((14, 0.5))
            o2.obtained_positions.append((86, 0.5))

            self.assertNotEquals(pm.value('GOOG'), 1)
            self.assertNotEquals(pm.value('GOOG'), 0.5)

            # order 3
            o3 = MarketOrder(Type.BUY, 'AAPL', 80)
            o3.fulfill_time = datetime.datetime.now()
            o3.obtained_positions.append((14, 0.2))
            o3.obtained_positions.append((86, 0.2))

            e3 = threading.Event()
            pm.portfolio_value_update += lambda x: e3.set()
            pm.on_event({'type': 'order_fulfilled', 'order': o3})
            e3.wait()

            self.assertNotEquals(pm.value('GOOG'), 1)
            self.assertNotEquals(pm.value('GOOG'), 0.5)
            self.assertNotEquals(pm.value('AAPL'), 0.2)
            self.assertEqual(len(pm._values), 2)

if __name__ == '__main__':
    unittest.main()
