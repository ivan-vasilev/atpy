import unittest
import threading

import pyevents.events as events
from atpy.environment import Environment


class TestEnvironment(unittest.TestCase):
    """
    Test Environment
    """

    def setUp(self):
        events.reset()

    def test_1(self):
        events.use_global_event_bus()
        e = threading.Event()
        with Environment(interval_len=300, mkt_snapshot_depth=3):
            e.wait()