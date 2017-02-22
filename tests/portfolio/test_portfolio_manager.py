import unittest
from atpy.portfolio.portfolio_manager import *
import pymongo
from pyevents.events import *
from pyeventsml.mongodb.mongodb_store import *


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
        self.assertEqual(pm.value('GOOG'), 100 * 24)
        self.assertEqual(len(pm.value()), 1)
        self.assertEqual(pm.value()['GOOG'], 100 * 24)
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
        self.assertEqual(pm.value('GOOG'), 250 * 27)
        self.assertEqual(len(pm.value()), 1)
        self.assertEqual(pm.value()['GOOG'], 250 * 27)
        self.assertEqual(pm.capital, 10000 - (14 * 23 + 86 * 24 + 110 * 25 + 30 * 26 + 10 * 27))

        # order 3
        o1 = MarketOrder(Type.SELL, 'GOOG', 60)
        o1.fulfill_time = datetime.datetime.now()
        o1.obtained_positions.append((60, 22))

        pm.on_event({'type': 'order_fulfilled', 'order': o1})

        self.assertEqual(len(pm.quantity()), 1)
        self.assertEqual(pm.quantity()['GOOG'], 190)
        self.assertEqual(pm.quantity('GOOG'), 190)
        self.assertEqual(pm.value('GOOG'), 190 * 22)
        self.assertEqual(len(pm.value()), 1)
        self.assertEqual(pm.value()['GOOG'], 190 * 22)
        self.assertEqual(pm.capital, 10000 - (14 * 23 + 86 * 24 + 110 * 25 + 30 * 26 + 10 * 27) + 60 * 22)

        # order 4
        o1 = MarketOrder(Type.BUY, 'AAPL', 50)
        o1.fulfill_time = datetime.datetime.now()
        o1.obtained_positions.append((50, 21))

        pm.on_event({'type': 'order_fulfilled', 'order': o1})

        self.assertEqual(len(pm.quantity()), 2)
        self.assertEqual(pm.quantity()['AAPL'], 50)
        self.assertEqual(pm.quantity('AAPL'), 50)
        self.assertEqual(pm.value('AAPL'), 50 * 21)
        self.assertEqual(len(pm.value()), 2)
        self.assertEqual(pm.value()['AAPL'], 50 * 21)
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

if __name__ == '__main__':
    unittest.main()
