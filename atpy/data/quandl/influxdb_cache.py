import datetime
import logging

import numpy as np
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
        chunksize = 100000
        for i, df in enumerate(bulkdownload(dataset=dataset, chunksize=chunksize)):
            df.reset_index(level=['symbol', 'indicator', 'dimension'], inplace=True)
            self.client.write_points(df, 'quandl_' + dataset, protocol='line', time_precision='s')

            if i > 0 and (i % 5 == 0 or len(df) < chunksize):
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

            cols = [c for c in result.columns if c != 'value']

            result.set_index(cols, drop=True, inplace=True, append=True)
            result.index.rename('date', level=0, inplace=True)
            result.sort_index(inplace=True)

            if result['value'].dtype != np.float:
                result['value'] = result['value'].astype(np.float)
        else:
            result = None

        return result
