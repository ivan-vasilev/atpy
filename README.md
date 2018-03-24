# Event-based Algorithmic Trading For Python 
 
Event-based Algorithmic trading library. The events implementation is [pyevents](https://github.com/ivan-vasilev/pyevents)
 library. The features are:
 
* Real-time and historical bar and tick data from [IQFeed](http://www.iqfeed.net/) via [@pyiqfeed](https://github.com/akapur/pyiqfeed). The data is provided as pandas multiindex dataframes. For this to work, you need IQFeed subscription.
* API integration with [Quandl](https://www.quandl.com/) and [INTRINIO](https://intrinio.com/). 
* Storing and retrieving historical data and other datasets with [PostgreSQL](https://www.postgresql.org) and [InfluxDB](https://www.influxdata.com/). Again, the data is provided via pandas dataframes.
* Placing orders via the [Interactive Brokers Python API](https://github.com/InteractiveBrokers/tws-api-public). For this to work, you need to have IB account.

For more information on how to use the library please check the unit tests. 

#### Author
Ivan Vasilev (ivanvasilev [at] gmail (dot) com)

#### License
[MIT License](http://opensource.org/licenses/MIT)