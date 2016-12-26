from atpy.data.data_provider import *

from abc import *
import pyiqfeed as iq
import datetime
from passwords import dtn_product_id, dtn_login, dtn_password


class IQFeedNewsProvider(DataProvider):

    def launch_service(self):
        """Check if IQFeed.exe is running and start if not"""

        svc = iq.FeedService(product=dtn_product_id,
                             version="Debugging",
                             login=dtn_login,
                             password=dtn_password)
        svc.launch()

    def __iter__(self):
        self.launch_service()

        news_conn = iq.NewsConn("pyiqfeed-example-News-Conn")
        news_listener = iq.VerboseIQFeedListener("NewsListener")
        news_conn.add_listener(news_listener)

        with iq.ConnConnector([news_conn]) as connector:
            cfg = news_conn.request_news_config()
            print("News Configuration:")
            print(cfg)
            print("")

            print("Latest 10 headlines:")
            headlines = news_conn.request_news_headlines(
                sources=[], symbols=[], date=None, limit=10)
            print(headlines)
            print("")

            story_id = headlines[0].story_id
            story = news_conn.request_news_story(story_id)
            print("Text of story with story id: %s:" % story_id)
            print(story.story)
            print("")

            today = datetime.date.today()
            week_ago = today - datetime.timedelta(days=7)

            counts = news_conn.request_story_counts(
                symbols=["AAPL", "IBM", "TSLA"],
                bgn_dt=week_ago, end_dt=today)
            print("Number of news stories in last week for AAPL, IBM and TSLA:")
            print(counts)
            print("")

    def __next__(self) -> map:
        return map()
