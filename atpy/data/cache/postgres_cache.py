import datetime
import logging
import queue
import threading
import typing
from functools import partial
from io import StringIO

import pandas as pd
import psycopg2
from dateutil import tz
from dateutil.relativedelta import relativedelta

from atpy.data.ts_util import slice_periods


class BarsFilter(typing.NamedTuple):
    ticker: typing.Union[list, str]
    interval_len: int
    interval_type: str
    bgn_prd: datetime.datetime


create_bars = \
    """
    -- Table: public.{0}

    DROP TABLE IF EXISTS public.{0};

    CREATE TABLE public.{0}
    (
        "timestamp" timestamp without time zone NOT NULL,
        symbol character varying COLLATE pg_catalog."default" NOT NULL,
        open real NOT NULL,
        high real NOT NULL,
        low real NOT NULL,
        close real NOT NULL,
        total_volume integer NOT NULL,
        period_volume integer NOT NULL,
        number_of_trades integer NOT NULL,
        "interval" character varying COLLATE pg_catalog."default" NOT NULL
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
    """

bars_indices = \
    """
    -- Constraint: {0}_pkey

    -- ALTER TABLE public.{0} DROP CONSTRAINT {0}_pkey;

    ALTER TABLE public.{0}
        ADD CONSTRAINT {0}_pkey PRIMARY KEY ("timestamp", symbol);

    -- Index: {0}_symbol_ind

    -- DROP INDEX public.{0}_symbol_ind;

    CREATE INDEX {0}_symbol_ind
        ON public.{0} USING btree
        (symbol COLLATE pg_catalog."default")
        TABLESPACE pg_default;

    -- Index: {0}_timestamp_ind

    -- DROP INDEX public.{0}_timestamp_ind;

    CREATE INDEX {0}_timestamp_ind
        ON public.{0} USING btree
        ("timestamp")
        TABLESPACE pg_default;

    -- Index: interval_ind
    
    -- DROP INDEX public.{0}_interval_ind;
    
    CREATE INDEX {0}_interval_ind
        ON public.{0} USING btree
        ("interval" COLLATE pg_catalog."default")
        TABLESPACE pg_default;
    """

create_adjustments = \
    """
    -- Table: public.{0}

    DROP TABLE IF EXISTS public.{0};

    CREATE TABLE public.{0}
    (
        "timestamp" timestamp without time zone NOT NULL,
        symbol character varying COLLATE pg_catalog."default" NOT NULL,
        "type" character varying COLLATE pg_catalog."default" NOT NULL,
        provider character varying COLLATE pg_catalog."default" NOT NULL,
        value real NOT NULL
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
    """

adjustments_indices = \
    """
    -- Table: public.{0}

    ALTER TABLE public.{0}
        ADD CONSTRAINT {0}_pkey PRIMARY KEY ("timestamp", symbol, type, provider);

    -- Index: {0}_symbol_ind
    
    -- DROP INDEX public.{0}_symbol_ind;
    
    CREATE INDEX {0}_symbol_ind
        ON public.{0} USING btree
        (symbol COLLATE pg_catalog."default")
        TABLESPACE pg_default;
    
    -- Index: {0}_timestamp_ind

    -- DROP INDEX public.{0}_timestamp_ind;
    
    CREATE INDEX {0}_timestamp_ind
        ON public.{0} USING btree
        ("timestamp")
        TABLESPACE pg_default;
    
    -- Index: {0}_type_ind
    
    -- DROP INDEX public.{0}_type_ind;
    
    CREATE INDEX {0}_type_ind
        ON public.{0} USING btree
        (type COLLATE pg_catalog."default")
        TABLESPACE pg_default;
    """


def update_to_latest(url: str, bars_table: str, noncache_provider: typing.Callable, symbols: set = None, time_delta_back: relativedelta = relativedelta(years=5), skip_if_older_than: relativedelta = None):
    con = psycopg2.connect(url)
    con.autocommit = True
    cur = con.cursor()

    cur.execute("SELECT to_regclass('public.{0}')".format(bars_table))

    exists = [t for t in cur][0][0] is not None

    if not exists:
        cur.execute(create_bars.format(bars_table))

    ranges = pd.read_sql("select symbol, max(timestamp) as timestamp, interval from {0} group by symbol, interval".format(bars_table), con=con, index_col=['symbol'])
    if not ranges.empty:
        ranges['timestamp'] = ranges['timestamp'].dt.tz_localize('UTC')

    new_symbols = set() if symbols is None else symbols

    if skip_if_older_than is not None:
        skip_if_older_than = datetime.datetime.utcnow().replace(tzinfo=tz.gettz('UTC')) - skip_if_older_than

    filters = list()
    for row in ranges.iterrows():
        interval_len, interval_type = int(row[1][1].split('_')[0]), row[1][1].split('_')[1]

        if (row[0], interval_len, interval_type) in new_symbols:
            new_symbols.remove((row[0], interval_len, interval_type))

        bgn_prd = row[1][0].to_pydatetime()

        if skip_if_older_than is None or bgn_prd > skip_if_older_than:
            filters.append(BarsFilter(ticker=row[0], bgn_prd=bgn_prd, interval_len=interval_len, interval_type=interval_type))

    bgn_prd = datetime.datetime.combine(datetime.datetime.utcnow().date() - time_delta_back, datetime.datetime.min.time()).replace(tzinfo=tz.gettz('UTC'))
    for (symbol, interval_len, interval_type) in new_symbols:
        filters.append(BarsFilter(ticker=symbol, bgn_prd=bgn_prd, interval_len=interval_len, interval_type=interval_type))

    logging.getLogger(__name__).info("Updating " + str(len(filters)) + " total symbols and intervals; New symbols and intervals: " + str(len(new_symbols)))

    q = queue.Queue(maxsize=100)

    threading.Thread(target=partial(noncache_provider, filters=filters, q=q), daemon=True).start()

    global_counter = {"counter": 0}

    def worker():
        con = psycopg2.connect(url)
        con.autocommit = True

        while True:
            tupl = q.get()

            if tupl is None:
                q.put(None)
                return

            ft, df = tupl
            try:
                # Prepare data
                del df['timestamp']
                df['interval'] = str(ft.interval_len) + '_' + ft.interval_type

                insert_df(con, bars_table, df)
            except Exception as err:
                logging.getLogger(__name__).error("Error loading " + ft.ticker)
                logging.getLogger(__name__).exception(err)

            global_counter['counter'] += 1
            i = global_counter['counter']

            if i > 0 and (i % 20 == 0 or i == len(filters)):
                logging.getLogger(__name__).info("Cached " + str(i) + " queries")

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    if not exists:
        cur.execute(bars_indices.format(bars_table))


def request_bars(conn, bars_table: str, interval_len: int, interval_type: str, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, ascending=True, selection='*'):
    """
    Request bar data
    :param conn: connection
    :param bars_table: table name
    :param interval_len: interval len
    :param interval_type: interval type
    :param symbol: symbol or a list of symbols
    :param bgn_prd: start period (including)
    :param end_prd: end period (excluding)
    :param ascending: asc/desc
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

    if interval_len is not None and interval_type is not None:
        where += " AND interval = %s"
        params.append(str(interval_len) + '_' + interval_type)

    if bgn_prd is not None:
        where += " AND timestamp >= %s"
        params.append(str(bgn_prd))

    if end_prd is not None:
        where += " AND timestamp <= %s"
        params.append(str(end_prd))

    sort = 'ASC' if ascending else 'DESC'

    df = pd.read_sql("SELECT " + selection + " FROM " + bars_table + where + " ORDER BY timestamp " + sort + ", symbol", con=conn, index_col=['timestamp', 'symbol'], params=params)

    if not df.empty:
        del df['interval']

        df.tz_localize('UTC', level=0, copy=False)

        if isinstance(symbol, str):
            df.reset_index(level=1, inplace=True, drop=True)

        df['period_volume'] = df['period_volume'].astype('uint64')
        df['total_volume'] = df['total_volume'].astype('uint64')
        df['number_of_trades'] = df['number_of_trades'].astype('uint64')

    return df


def insert_df(conn, table_name: str, df: pd.DataFrame):
    """
    add a list of splits/dividends to the database
    :param conn: db connection
    :param table_name: table name
    :param df: list of adjustments of the type [(timestamp: datetime.date, symbol: str, typ: str, value), ...]
    """
    # To CSV
    output = StringIO()
    df.to_csv(output, sep='\t', header=False)
    output.seek(0)

    # Insert data
    cursor = conn.cursor()

    if isinstance(df.index, pd.MultiIndex):
        columns = list(df.index.names) + list(df.columns)
    else:
        columns = [df.index.name] + list(df.columns)

    cursor.copy_from(output, table_name, sep='\t', null='', columns=columns)
    conn.commit()
    cursor.close()


def request_adjustments(conn, table_name: str, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, provider: str = None):
    """
    add a list of splits/dividends to the database
    :param conn: db connection
    :param table_name: db connection
    :param symbol: symbol / list of symbols
    :param bgn_prd: begin period
    :param end_prd: end period
    :param provider: data provider
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
        where += " AND timestamp >= %s"
        params.append(str(bgn_prd))

    if end_prd is not None:
        where += " AND timestamp <= %s"
        params.append(str(end_prd))

    if provider is not None:
        where += " AND provider = %s"
        params.append(provider)

    df = pd.read_sql("SELECT * FROM " + table_name + where + " ORDER BY timestamp ASC, symbol", con=conn, index_col=['timestamp', 'symbol', 'type', 'provider'], params=params)

    if not df.empty:
        df.tz_localize('UTC', level=0, copy=False)
        df.sort_index(inplace=True)

    return df


class BarsInPeriodProvider(object):
    """
    OHLCV Bars in period provider
    """

    def __init__(self, conn, bars_table: str, bgn_prd: datetime.datetime, delta: relativedelta, interval_len: int, interval_type: str, symbol: typing.Union[list, str] = None, ascend: bool = True, overlap: relativedelta = None):
        self._periods = slice_periods(bgn_prd=bgn_prd, delta=delta, ascend=ascend, overlap=overlap)

        self.conn = conn
        self.bars_table = bars_table
        self.interval_len = interval_len
        self.interval_type = interval_type
        self.symbol = symbol
        self.ascending = ascend

    def __iter__(self):
        self._deltas = -1
        return self

    def __next__(self):
        self._deltas += 1

        if self._deltas < len(self._periods):
            return self._request(*self._periods[self._deltas])
        else:
            raise StopIteration

    def _request(self, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None):
        return request_bars(conn=self.conn, bars_table=self.bars_table, symbol=self.symbol, interval_len=self.interval_len, interval_type=self.interval_type, bgn_prd=bgn_prd, end_prd=end_prd, ascending=self.ascending)
