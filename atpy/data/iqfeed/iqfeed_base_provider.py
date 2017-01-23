from atpy.data.data_provider import *

from abc import *
import pyiqfeed as iq
from passwords import dtn_product_id, dtn_login, dtn_password


def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = iq.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch()


class IQFeedBaseProvider(DataProvider, metaclass=ABCMeta):

    def __enter__(self):
        """Handle connection->connect etc"""
        launch_service()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        return

    def __iter__(self):
        launch_service()

        return self
