__author__ = 'Qing'

import datetime
import trading_date_utility

trading_date= datetime.datetime.now().date()
start_date = trading_date_utility.nearest_trading_day(trading_date,"US")
print start_date