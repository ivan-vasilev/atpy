from influxdb import DataFrameClient


def influxdb_ohlcv_environment(listeners, client: DataFrameClient, interval_len: int, interval_type: str = 's'):
    pass


class __ContextManagerEnvironment(object):
    def __init__(self, context_managers: list):
        self.context_managers = context_managers

    def __enter__(self):
        for cm in self.context_managers:
            cm.__enter__()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for cm in self.context_managers:
            cm.__exit__(exc_type=exc_type, exc_value=exc_value, traceback=traceback)
