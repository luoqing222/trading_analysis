__author__ = 'qingluo'

import wiki_data_loader
import trading_data_manager
import datetime
import trading_data_utility

if __name__ == "__main__":
    data_manager = trading_data_manager.TradingDataManager()
    #data_manager.populate_Sp500(datetime.date(2015, 4, 7))
    #data_manager.updateSp500(datetime.date(2015, 4, 9))

    data_utility = trading_data_utility.TradingDataUtility
    #print data_utility.get_sp500_list(datetime.date(2015, 4, 9))
    data_utility.create_strong_stock_table()
