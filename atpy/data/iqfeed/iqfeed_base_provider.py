from atpy.data.data_provider import *

from abc import *
import pyiqfeed as iq
from passwords import dtn_product_id, dtn_login, dtn_password


class IQFeedBaseProvider(DataProvider, metaclass=ABCMeta):

    @staticmethod
    def launch_service():
        """Check if IQFeed.exe is running and start if not"""

        svc = iq.FeedService(product=dtn_product_id,
                             version="Debugging",
                             login=dtn_login,
                             password=dtn_password)
        svc.launch()

    def __enter__(self):
        """Handle connection->connect etc"""
        self.launch_service()
        return self

    @abstractmethod
    def __exit__(self, exception_type, exception_value, traceback):
        """Disconnect connection etc"""
        return

    def __iter__(self):
        self.launch_service()

        return self
