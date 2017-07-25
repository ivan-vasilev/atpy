import threading
import unittest

import pyevents.events as events
from atpy.portfolio.order import *
from atpy.ibapi.ib_events import IBEvents


class TestIBApi(unittest.TestCase):
    """
    Test IB API Orders
    """

    def setUp(self):
        events.reset()

    def test_market_order(self):
        events.use_global_event_bus()

        ibe = IBEvents("127.0.0.1", 4002, 0)
        with ibe:
            e1 = threading.Event()
            events.listener(lambda x: e1.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'GOOG' else None)
            ibe.on_event({'type': 'order_request', 'data': MarketOrder(Type.BUY, 'GOOG', 1)})

            e2 = threading.Event()
            events.listener(lambda x: e1.set() if x['type'] == 'order_fulfilled' and x['data'].symbol == 'AAPL' else None)
            ibe.on_event({'type': 'order_request', 'data': MarketOrder(Type.BUY, 'AAPL', 1)})

            e1.wait()
            e2.wait()
