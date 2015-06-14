__author__ = 'Qing'

import trading_data_utility
import time
import datetime
import os

if __name__ == "__main__":
    start_time = time.time()
    current_date=datetime.datetime.now().date()
    data_utility=trading_data_utility.TradingDataUtility()
    data_utility.create_sp500_list()

    print("--- %s seconds ---" % (time.time() - start_time))
