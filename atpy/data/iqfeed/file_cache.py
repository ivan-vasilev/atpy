import pickle
import datetime
import typing
from atpy.data.iqfeed.iqfeed_history_provider import IQFeedHistoryListener, BarsInPeriodProvider
from atpy.data.util import get_us_listed_companies
import glob


def data_provider_to_file(prefix: str, provider, overwrite=False):
    if not overwrite:
        existing = glob.glob(prefix + '*.history_cache')

    for i, d in enumerate(provider):
        with open(prefix + '{0:03d}.history_cache'.format(i + 1 if overwrite else i + len(existing) + 1), 'wb') as f:
            pickle.dump(d, f, pickle.HIGHEST_PROTOCOL)


def bars_to_file(prefix: str, ticker: typing.Union[list, str], interval_len: int, interval_type: str, bgn_prd: datetime.date, delta: datetime.timedelta, ascend: bool=True, overwrite=False):
    filter_provider = BarsInPeriodProvider(ticker=ticker, bgn_prd=bgn_prd, delta=delta, interval_len=interval_len, interval_type=interval_type, ascend=ascend)
    if not overwrite:
        existing = glob.glob(prefix + '*.history_cache')
        if len(existing) > 0:
            fp = filter_provider

            def gen():
                for i, f in enumerate(fp):
                    if i >= len(existing):
                        yield f

            filter_provider = gen()

    with IQFeedHistoryListener(run_async=False, filter_provider=filter_provider, lmdb_path=None) as history:
        data_provider_to_file(prefix, history.next_batch(), overwrite=overwrite)


def all_bars_to_file(prefix: str, interval_len: int, interval_type: str, bgn_prd: datetime.date, delta: datetime.timedelta, ascend: bool=True):
    symbols = get_us_listed_companies()
    bars_to_file(prefix, ticker=list(symbols[0]), bgn_prd=bgn_prd, delta=delta, interval_len=interval_len, interval_type=interval_type, ascend=ascend)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    now = datetime.datetime.now()
    all_bars_to_file('5_min_all_symbols/df_', bgn_prd=datetime.date(now.year - 7, 1, 1), delta=datetime.timedelta(days=20), interval_len=300, ascend=True, interval_type='s')