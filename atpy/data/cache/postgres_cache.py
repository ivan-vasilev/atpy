import datetime
import logging
import os
import queue
import threading
import typing
from functools import partial
from io import StringIO

import pandas as pd
import psycopg2
from dateutil import tz
from dateutil.relativedelta import relativedelta

from atpy.data.cache.lmdb_cache import write
from atpy.data.ts_util import slice_periods


class BarsFilter(typing.NamedTuple):
    ticker: str
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
        period_volume integer NOT NULL,
        "interval" character varying COLLATE pg_catalog."default" NOT NULL
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
    """

bars_indices = \
    """
    -- Index: {0}_timestamp_ind

    -- DROP INDEX public.{0}_timestamp_ind;

    CREATE INDEX {0}_timestamp_ind
        ON public.{0} USING btree
        ("timestamp")
        TABLESPACE pg_default;

    ALTER TABLE public.{0}
            CLUSTER ON {0}_timestamp_ind;

    CLUSTER {0};

    -- Index: {0}_symbol_ind

    -- DROP INDEX public.{0}_symbol_ind;

    CREATE INDEX {0}_symbol_ind
        ON public.{0} USING btree
        (symbol COLLATE pg_catalog."default")
        TABLESPACE pg_default;

    -- Index: interval_ind
    
    -- DROP INDEX public.{0}_interval_ind;
    
    CREATE INDEX {0}_interval_ind
        ON public.{0} USING btree
        ("interval" COLLATE pg_catalog."default")
        TABLESPACE pg_default;
    """


def update_to_latest(url: str, bars_table: str, noncache_provider: typing.Callable, symbols: set = None, time_delta_back: relativedelta = relativedelta(years=5), skip_if_older_than: relativedelta = None, cluster: bool = False):
    con = psycopg2.connect(url)
    con.autocommit = True
    cur = con.cursor()

    cur.execute("SELECT to_regclass('public.{0}')".format(bars_table))

    exists = [t for t in cur][0][0] is not None

    if not exists:
        cur.execute(create_bars.format(bars_table))

    if exists:
        logging.getLogger(__name__).info("Skim off the top...")
        cur.execute("delete from {0} where (symbol, timestamp, interval) in (select symbol, max(timestamp) as timestamp, interval from {0} group by symbol, interval)".format(bars_table))

    logging.getLogger(__name__).info("Ranges...")
    ranges = pd.read_sql("select symbol, max(timestamp) as timestamp, interval from {0} group by symbol, interval".format(bars_table), con=con, index_col=['symbol'])
    if not ranges.empty:
        ranges['timestamp'] = ranges['timestamp'].dt.tz_localize('UTC')

    new_symbols = set() if symbols is None else symbols

    if skip_if_older_than is not None:
        skip_if_older_than = datetime.datetime.utcnow().replace(tzinfo=tz.gettz('UTC')) - skip_if_older_than

    filters = dict()
    for row in ranges.iterrows():
        interval_len, interval_type = int(row[1][1].split('_')[0]), row[1][1].split('_')[1]

        if (row[0], interval_len, interval_type) in new_symbols:
            new_symbols.remove((row[0], interval_len, interval_type))

        bgn_prd = row[1][0].to_pydatetime()

        if skip_if_older_than is None or bgn_prd > skip_if_older_than:
            filters[BarsFilter(ticker=row[0], bgn_prd=bgn_prd, interval_len=interval_len, interval_type=interval_type)] = None

    bgn_prd = datetime.datetime.combine(datetime.datetime.utcnow().date() - time_delta_back, datetime.datetime.min.time()).replace(tzinfo=tz.gettz('UTC'))
    for (symbol, interval_len, interval_type) in new_symbols:
        filters[BarsFilter(ticker=symbol, bgn_prd=bgn_prd, interval_len=interval_len, interval_type=interval_type)] = None

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

            ft, df = filters[tupl[0]], tupl[1]

            # Prepare data
            for c in [c for c in df.columns if c not in ['symbol', 'open', 'high', 'low', 'close', 'period_volume']]:
                del df[c]

            df['interval'] = str(ft.interval_len) + '_' + ft.interval_type

            if df.iloc[0].name == ft.bgn_prd:
                df = df.iloc[1:]

            try:
                insert_df(con, bars_table, df)
            except Exception as err:
                logging.getLogger(__name__).error("Error saving " + ft.ticker)
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

    logging.getLogger(__name__).info("Done inserting data")

    if not exists:
        logging.getLogger(__name__).info("Creating indices...")
        cur.execute(bars_indices.format(bars_table))
    elif cluster:
        logging.getLogger(__name__).info("Cluster...")
        cur.execute("CLUSTER {0}".format(bars_table))


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
    where, params = __bars_query_where(interval_len=interval_len, interval_type=interval_type, symbol=symbol, bgn_prd=bgn_prd, end_prd=end_prd)

    sort = 'ASC' if ascending else 'DESC'

    df = pd.read_sql("SELECT " + selection + " FROM " + bars_table + where + " ORDER BY timestamp " + sort + ", symbol", con=conn, index_col=['timestamp', 'symbol'], params=params)

    if not df.empty:
        del df['interval']

        df.tz_localize('UTC', level='timestamp', copy=False)

        if isinstance(symbol, str):
            df.reset_index(level='symbol', inplace=True, drop=True)

        for c in [c for c in ['period_volume', 'total_volume', 'number_of_trades'] if c in df.columns]:
            df[c] = df[c].astype('uint64')

    return df


def request_symbol_counts(conn, bars_table: str, interval_len: int, interval_type: str, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None):
    """
    Request number of bars for each symbol
    :param conn: connection
    :param bars_table: table name
    :param interval_len: interval len
    :param interval_type: interval type
    :param bgn_prd: start period (including)
    :param end_prd: end period (excluding)
    :return: series
    """
    where, params = __bars_query_where(interval_len=interval_len, interval_type=interval_type, symbol=None, bgn_prd=bgn_prd, end_prd=end_prd)

    result = pd.read_sql("SELECT symbol, count(*) as count FROM " + bars_table + where + " GROUP BY symbol", con=conn, index_col='symbol', params=params)

    if not result.empty:
        result['count'] = result['count'].astype('uint64')

    return result['count']


def __bars_query_where(interval_len: int, interval_type: str, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None):
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

    return where, params


def insert_df(conn, table_name: str, df: pd.DataFrame):
    """
    insert dataframe to the database
    :param conn: db connection
    :param table_name: table name
    :param df: dataframe to insert
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


def insert_df_json(conn, table_name: str, df: pd.DataFrame):
    """
    insert dataframe in json table
    :param conn: db connection
    :param table_name: table name
    :param df: list of adjustments of the type [(timestamp: datetime.date, symbol: str, typ: str, value), ...]
    """
    insert_json(conn=conn, table_name=table_name, data=df.reset_index().to_json(orient='records', lines=True))


def insert_json(conn, table_name: str, data: str):
    """
    insert json data
    :param conn: db connection
    :param table_name: table name
    :param data: json string (or strings, separated by new line character)
    """
    output = StringIO(data)

    # Insert data
    cursor = conn.cursor()

    cursor.copy_from(output, table_name, null='', columns=['json_data'])
    conn.commit()
    cursor.close()


def request_adjustments(conn, table_name: str, symbol: typing.Union[list, str] = None, bgn_prd: datetime.datetime = None, end_prd: datetime.datetime = None, adj_type: str = None, provider: str = None):
    """
    add a list of splits/dividends to the database
    :param conn: db connection
    :param table_name: db connection
    :param symbol: symbol / list of symbols
    :param bgn_prd: begin period
    :param end_prd: end period
    :param provider: data provider
    :param adj_type: adjustment type (split/dividend)
    """
    where = " WHERE 1=1"
    params = list()

    if isinstance(symbol, list):
        where += " AND json_data ->> 'symbol' IN (%s)" % ','.join(['%s'] * len(symbol))
        params += symbol
    elif isinstance(symbol, str):
        where += " AND json_data ->> 'symbol' = %s"
        params.append(symbol)

    if bgn_prd is not None:
        where += " AND CAST(json_data ->> 'timestamp' AS BIGINT) >= %s"
        params.append(int(bgn_prd.timestamp() * 1000))

    if end_prd is not None:
        where += " AND CAST(json_data ->> 'timestamp' AS BIGINT) <= %s"
        params.append(int(end_prd.timestamp() * 1000))

    if provider is not None:
        where += " AND json_data ->> 'provider' = %s"
        params.append(provider)

    if adj_type is not None:
        where += " AND json_data ->> 'type' = %s"
        params.append(adj_type)
    else:
        where += " AND json_data ->> 'type' in ('split', 'dividend')"

    cursor = conn.cursor()
    cursor.execute("select * from {0} {1}".format(table_name, where), params)
    records = cursor.fetchall()

    if len(records) > 0:
        df = pd.DataFrame([x[0] for x in records])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index(keys=['timestamp', 'symbol', 'type', 'provider'], drop=True, append=False, inplace=True)
        df.tz_localize('UTC', level='timestamp', copy=False)
        df.sort_index(inplace=True)

    return df


class BarsInPeriodProvider(object):
    """
    OHLCV Bars in period provider
    """

    def __init__(self, conn, bars_table: str, bgn_prd: datetime.datetime, delta: relativedelta, interval_len: int, interval_type: str, symbol: typing.Union[list, str] = None, ascend: bool = True, overlap: relativedelta = None,
                 cache: typing.Callable = None):
        self._periods = slice_periods(bgn_prd=bgn_prd, delta=delta, ascend=ascend, overlap=overlap)

        self.conn = conn
        self.bars_table = bars_table
        self.interval_len = interval_len
        self.interval_type = interval_type
        self.symbol = symbol
        self.ascending = ascend
        self.cache = cache

    def __iter__(self):
        self._deltas = -1
        return self

    def __next__(self):
        self._deltas += 1

        if self._deltas < len(self._periods):
            if self.cache is not None:
                result = self.cache(self.current_cache_key())
                if result is not None:
                    return result

            return request_bars(conn=self.conn, bars_table=self.bars_table, symbol=self.symbol, interval_len=self.interval_len, interval_type=self.interval_type, bgn_prd=self._periods[self._deltas][0],
                                end_prd=self._periods[self._deltas][1], ascending=self.ascending)
        else:
            raise StopIteration

    def current_cache_key(self):
        return str(self._periods[self._deltas][0]) + ' - ' + str(self._periods[self._deltas][1]) + ' - ' + str(self.interval_len) + str(self.interval_type)


def bars_to_lmdb(provider: BarsInPeriodProvider, lmdb_path: str = None):
    if lmdb_path is None:
        lmdb_path = os.environ['ATPY_LMDB_PATH']

    for df in provider:
        write(provider.current_cache_key(), df, lmdb_path)


create_json_data = \
    """
    CREATE TABLE public.{0}
    (
        json_data jsonb NOT NULL
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
    """
