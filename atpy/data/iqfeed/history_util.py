import atpy.data.util as data_util
from atpy.data.iqfeed.iqfeed_history_provider import *


def get_bar_mean_std(symbols: typing.Union[list, str]=None, interaval_len=10000, interval_type='d', skip_zeros=True, years_back=10, lmdb_path=None):
    """
    get mean and std values for bar data
    :param symbols: symbol or list of symbols
    :param interaval_len: length of the interval (WORKS ONLY IF interval_type is 's')
    :param interval_type: 's' for seconds, 'd' for days, 'w' for weeks, 'm' for months
    :param skip_zeros: exclude zero values from the computation
    :param years_back: number of years to use for the computation
    :paral lmdb_path: path to lmdb file
    """
    if symbols is None:
        nd = data_util.get_nasdaq_listed_companies()
        nd = nd[nd['Financial Status'] == 'N']
        non_nd = data_util.get_non_nasdaq_listed_companies()
        symbols = list(set(list(non_nd['ACT Symbol']) + list(nd['symbol'])))
        symbols.sort()

    if isinstance(symbols, str):
        symbols = [symbols]

    db = lmdb.open(lmdb_path if lmdb_path is not None else os.path.join(os.path.abspath('../' * (len(__name__.split('.')) - 2)), 'data', 'cache', 'mean_std'))

    mean_std = list()

    non_cached_symbols = list()

    key_start = '_' + str(interaval_len) + '_' + interval_type + '_' + str(skip_zeros)
    for s in symbols:
        with db.begin() as txn:
            data = txn.get(bytearray(s + key_start, encoding='ascii'))

        if data is not None:
            mean_std.append(pickle.loads(data))
        else:
            non_cached_symbols.append(s)

    if len(non_cached_symbols) > 0:
        with IQFeedHistoryListener(run_async=False) as history:
            if interval_type in ['d', 'w', 'm']:
                if interval_type == 'd':
                    data = history.request_data(BarsDailyFilter(ticker=non_cached_symbols, num_days=365 * years_back), synchronize_timestamps=False)
                elif interval_type == 'w':
                    data = history.request_data(BarsWeeklyFilter(ticker=non_cached_symbols, num_weeks=52 * years_back), synchronize_timestamps=False)
                elif interval_type == 'm':
                    data = history.request_data(BarsMonthlyFilter(ticker=non_cached_symbols, num_months=12 * years_back), synchronize_timestamps=False)

                if data is not None:
                    for s in data:
                        c_o = data[s]['close'] - data[s]['open']
                        h_l = data[s]['high'] - data[s]['low']

                        if skip_zeros:
                            c_o = c_o[c_o != 0]
                            h_l = h_l[h_l != 0]

                        item = [s, c_o.mean(), c_o.std(), h_l.mean(), h_l.std()]

                        mean_std.append(item)

                        with db.begin(write=True) as txn:
                            txn.put(bytearray(s + key_start, encoding='ascii'), pickle.dumps(item))

            elif interval_type == 's':
                # mean value
                now = datetime.datetime.now()

                filter_provider = BarsInPeriodProvider(ticker=non_cached_symbols, interval_type=interval_type, interval_len=interaval_len, bgn_prd=datetime.date(now.year - years_back, 1, 1), delta=datetime.timedelta(days=122))

                sums = dict()

                for f in filter_provider:
                    data = history.request_data(f, synchronize_timestamps=False)
                    for k, v in data.items():
                        c_o = v['close'] - v['open']
                        h_l = v['high'] - v['low']

                        if skip_zeros:
                            c_o = c_o[c_o != 0]
                            h_l = h_l[h_l != 0]

                        if k not in sums:
                            sums[k] = [c_o.sum(), h_l.sum(), c_o.shape[0]]
                        else:
                            t = sums[k]
                            sums[k] = [t[0] + c_o.sum(), t[1] + h_l.mean(), t[2] + c_o.shape[0]]

                means = {k: [v[0] / v[2], v[1] / v[2]] for k, v in sums.items()}

                # standard deviation
                filter_provider = BarsInPeriodProvider(ticker=non_cached_symbols, interval_type=interval_type, interval_len=interaval_len, bgn_prd=datetime.date(now.year - years_back, 1, 1), delta=datetime.timedelta(days=122))

                sums = dict()

                for f in filter_provider:
                    data = history.request_data(f, synchronize_timestamps=False)
                    for k, v in data.items():
                        c_o = np.square(v['close'] - v['open'] - means[k][0])
                        h_l = np.square(v['high'] - v['low'] - means[k][1])

                        if skip_zeros:
                            c_o = c_o[c_o != 0]
                            h_l = h_l[h_l != 0]

                        if k not in sums:
                            sums[k] = (c_o.sum(), h_l.sum(), c_o.shape[0])
                        else:
                            t = sums[k]
                            sums[k] = (t[0] + c_o.sum(), t[1] + h_l.mean(), t[2] + c_o.shape[0])

                stds = {k: [v[0] / v[2], v[1] / v[2]] for k, v in sums.items()}

                # combine
                for k in stds:
                    item = [k] + means[k] + stds[k]
                    mean_std.append(item)

                    with db.begin(write=True) as txn:
                        txn.put(bytearray(k + key_start, encoding='ascii'), pickle.dumps(item))

    result = pd.DataFrame(mean_std)
    result.columns = ['Symbol', 'C-O-mean', 'C-O-std', 'H-L-mean', 'H-L-std']

    return result


def create_bar_history_cache(interaval_len: int, symbols: typing.Union[list, str]=None, years_back=10):
    """
    get mean and std values for bar data
    :param symbols: symbol or list of symbols
    :param interaval_len: length of the interval (WORKS ONLY IF interval_type is 's')
    :param years_back: number of years to use for the computation
    """
    if symbols is None:
        symbols = data_util.get_us_listed_companies()

    if isinstance(symbols, str):
        symbols = [symbols]

    if len(symbols) > 0:
        with IQFeedHistoryListener(run_async=False) as history:
            filter_provider = BarsInPeriodProvider(ticker=symbols, interval_type='s', interval_len=interaval_len, bgn_prd=datetime.date(datetime.datetime.now().year - years_back, 1, 1), delta=datetime.timedelta(days=121))

            for i, f in enumerate(filter_provider):
                history.request_data(f, synchronize_timestamps=False)
                logging.getLogger(__name__).info("Cached " + str(i + 1) + " filters, " + str(interaval_len * (i + 1)) + "s")
