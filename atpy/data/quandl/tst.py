import os

import quandl

api_key = os.environ['QUANDL_API_KEY'] if 'QUANDL_API_KEY' in os.environ else None
# quandl.bulkdownload("ZEA", api_key=api_key)
data_3 = quandl.bulkdownload("SF0", api_key=api_key)


# data_1 = quandl.get_table('MER/F1', compnumber="39102", api_key=api_key, paginate=True)
data_2 = quandl.get(["SF1/NKE_GP_MRQ", 'SF1/AAPL_GP_MRQ'], paginate=True, api_key=api_key)
# data_2 = quandl.get("SF1/NKE_GP_MRQ", paginate=True, api_key=api_key)
pass
