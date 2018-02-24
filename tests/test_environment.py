import unittest
import threading

import pyevents.events as events
from atpy.environments import Environment


class TestEnvironment(unittest.TestCase):
    """
    Test Environment
    """

    def test_1(self):
        e = threading.Event()
        with Environment(listeners=events.AsyncListeners(), interval_len=300, mkt_snapshot_depth=3, fire_news=True):
            e.wait()
