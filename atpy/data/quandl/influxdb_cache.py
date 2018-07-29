import datetime
import logging
import typing

import pandas as pd
from influxdb import DataFrameClient

from atpy.data.quandl.api import bulkdownload


class InfluxDBCache(object):
    """
    InfluxDB quandl cache using abstract data provider
    """

    def __init__(self, client: DataFrameClient, listeners=None):
        self.client = client
        self.listeners = listeners

    def __enter__(self):
        logging.basicConfig(level=logging.INFO)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.client.close()

    def add_dataset_to_cache(self, dataset: str):
        self.add_to_cache(measurement='quandl_' + dataset, dfs=bulkdownload(dataset=dataset, chunksize=100000))

    def add_to_cache(self, measurement: str, dfs: typing.Iterator[pd.DataFrame], tag_columns: list=None):
        for i, df in enumerate(dfs):
            self.client.write_points(df, 'quandl_' + measurement, tag_columns=tag_columns, protocol='line', time_precision='s')

            if i > 0 and i % 5 == 0:
                logging.getLogger(__name__).info("Cached " + str(i) + " queries")

        if i > 0 and i % 5 != 0:
            logging.getLogger(__name__).info("Cached " + str(i) + " queries")

    def request_data(self, dataset, tags: dict = None, start_date: datetime.date = None, end_date: datetime.date = None):
        query = "SELECT * FROM quandl_" + dataset

        where = list()
        if tags is not None:
            for t, v in tags.items():
                if isinstance(v, set) and len(v) > 0:
                    where.append(t + " =~ /{}/".format("|".join(['^' + s + '$' for s in v])))
                elif isinstance(v, str) and len(v) > 0:
                    where.append(t + " = '{}'".format(v))

        if start_date is not None:
            start_date = datetime.datetime.combine(start_date, datetime.datetime.min.time())
            where.append("time >= '{}'".format(start_date))

        if end_date is not None:
            end_date = datetime.datetime.combine(end_date, datetime.datetime.min.time())
            where.append("time <= '{}'".format(end_date))

        if len(where) > 0:
            query += " WHERE " + " AND ".join(where)

        result = self.client.query(query, chunked=True)

        if len(result) > 0:
            result = result["quandl_" + dataset]
            result.index.rename('date', inplace=True)
        else:
            result = None

        return result
