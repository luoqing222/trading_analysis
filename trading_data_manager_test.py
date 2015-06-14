__author__ = 'qingluo'

import trading_data_manager
import time
import datetime
import os

if __name__ == "__main__":

    start_time = time.time()
    current_date=datetime.datetime.now().date()
    data_manager=trading_data_manager.TradingDataManager()

    print data_manager.updateSp500(current_date)

    # file_name=os.path.dirname(__file__)+"/data/NASDAQ.txt"
    # data_manager.populate_NasdaqList(file_name,current_date)
    #
    # file_name=os.path.dirname(__file__)+"/data/NYSE.txt"
    # data_manager.populate_NYSEList(file_name, current_date)
    #
    # file_name=os.path.dirname(__file__)+"/data/INDEX.txt"
    # data_manager.populate_IndexList(file_name,current_date)

    print("--- %s seconds ---" % (time.time() - start_time))
