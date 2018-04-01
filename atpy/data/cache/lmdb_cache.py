import pickle
import zlib

import lmdb


def write(key: str, value, lmdb_path: str, compress=True):
    with lmdb.open(lmdb_path, map_size=int(1e10)) as lmdb_env, lmdb_env.begin(write=True) as lmdb_txn:
        lmdb_txn.put(key.encode(), zlib.compress(pickle.dumps(value)) if compress else pickle.dumps(value))


def read(key: str, lmdb_path: str, decompress=True):
    with lmdb.open(lmdb_path) as lmdb_env, lmdb_env.begin() as lmdb_txn:
        result = lmdb_txn.get(key.encode())
        if result is not None and decompress:
            result = zlib.decompress(result)

        return result


def read_pickle(key: str, lmdb_path: str, decompress=True):
    result = read(key=key, lmdb_path=lmdb_path, decompress=decompress)
    if result is not None:
        return pickle.loads(result)
