__author__ = 'qingluo'

import trading_data_manager
import time
import datetime

if __name__ == "__main__":

    start_time = time.time()
    current_date=datetime.datetime.now().date()
    #print current_date
    data_manager=trading_data_manager.TradingDataManager()

    file_name="NASDAQ.txt"
    data_manager.populate_NasdaqList(file_name,current_date)

    file_name="NYSE.txt"
    data_manager.populate_NYSEList(file_name, current_date)

    file_name="INDEX.txt"
    data_manager.populate_IndexList(file_name,current_date)

    print("--- %s seconds ---" % (time.time() - start_time))
