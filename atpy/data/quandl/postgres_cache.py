import datetime
import typing

import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta

from atpy.data.cache.postgres_cache import insert_df
from atpy.data.quandl.api import bulkdownload_sf
from atpy.data.ts_util import slice_periods


def bulkinsert(url: str, table_name: str, dataset: str):
    for data in bulkdownload_sf(dataset=dataset):
        con = psycopg2.connect(url)
        con.autocommit = True
        insert_df(con, table_name, data)


create_sf = \
    """
    -- Table: public.{0}
    
    -- DROP TABLE public.{0};
    
    CREATE TABLE public.{0}
    (
        date timestamp(6) without time zone NOT NULL,
        symbol character varying COLLATE pg_catalog."default" NOT NULL,
        indicator character varying COLLATE pg_catalog."default" NOT NULL,
        dimension character varying COLLATE pg_catalog."default" NOT NULL,
        value double precision
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
    
    ALTER TABLE public.{0}
        OWNER to postgres;
    
    """

create_sf_indices = \
    """
        -- Index: ix_{0}_date
        
        -- DROP INDEX public.ix_{0}_date;
        
        CREATE INDEX ix_{0}_date
            ON public.{0} USING btree
            (date)
            TABLESPACE pg_default;
        
        ALTER TABLE public.{0}
            CLUSTER ON ix_{0}_date;
        
        -- Index: ix_{0}_dimension
        
        -- DROP INDEX public.ix_{0}_dimension;
        
        CREATE INDEX ix_{0}_dimension
            ON public.{0} USING btree
            (dimension COLLATE pg_catalog."default")
            TABLESPACE pg_default;
        
        -- Index: ix_{0}_indicator
        
        -- DROP INDEX public.ix_{0}_indicator;
        
        CREATE INDEX ix_{0}_indicator
            ON public.{0} USING btree
            (indicator COLLATE pg_catalog."default")
            TABLESPACE pg_default;
        
        -- Index: ix_{0}_symbol
        
        -- DROP INDEX public.ix_{0}_symbol;
        
        CREATE INDEX ix_{0}_symbol
            ON public.{0} USING btree
            (symbol COLLATE pg_catalog."default")
            TABLESPACE pg_default;
    """


def bulkinsert_SF0(url: str, table_name: str = 'quandl_sf0'):
    con = psycopg2.connect(url)
    con.autocommit = True
    cur = con.cursor()

    cur.execute("DROP TABLE IF EXISTS {0};".format(table_name))

    cur.execute(create_sf.format(table_name))

    bulkinsert(url, table_name=table_name, dataset='SF0')

    cur.execute(create_sf_indices.format(table_name))


def request_sf(conn, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, table_name: str = 'quandl_SF0', selection='*'):
    """
    Request bar data
    :param conn: connection
    :param table_name: table name
    :param symbol: symbol or a list of symbols
    :param bgn_prd: start period (including)
    :param end_prd: end period (excluding)
    :param selection: what to select
    :return: dataframe
    """
    where = " WHERE 1=1"
    params = list()

    if isinstance(symbol, list):
        where += " AND symbol IN (%s)" % ','.join(['%s'] * len(symbol))
        params += symbol
    elif isinstance(symbol, str):
        where += " AND symbol = %s"
        params.append(symbol)

    if bgn_prd is not None:
        where += " AND date >= %s"
        params.append(str(bgn_prd))

    if end_prd is not None:
        where += " AND date <= %s"
        params.append(str(end_prd))

    df = pd.read_sql("SELECT " + selection + " FROM " + table_name + where + " ORDER BY date, symbol", con=conn, index_col=['date', 'symbol', 'indicator', 'dimension'], params=params)

    if not df.empty:
        df.tz_localize('UTC', level=0, copy=False)

    return df


class SFInPeriodProvider(object):
    """
    SF (0 or 1) dataset in period provider
    """

    def __init__(self, conn, bgn_prd: datetime.datetime, delta: relativedelta, symbol: typing.Union[list, str] = None, ascend: bool = True, table_name: str = 'quandl_SF0', overlap: relativedelta = None):
        self._periods = slice_periods(bgn_prd=bgn_prd, delta=delta, ascend=ascend, overlap=overlap)

        self.conn = conn
        self.symbol = symbol
        self.ascending = ascend
        self.table_name = table_name

    def __iter__(self):
        self._deltas = -1
        return self

    def __next__(self):
        self._deltas += 1

        if self._deltas < len(self._periods):
            return request_sf(conn=self.conn, symbol=self.symbol, bgn_prd=self._periods[self._deltas][0], end_prd=self._periods[self._deltas][1], table_name=self.table_name)
        else:
            raise StopIteration
