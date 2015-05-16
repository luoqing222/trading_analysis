__author__ = 'qingluo'

import wiki_data_loader
import trading_data_management
import datetime

if __name__ == "__main__":
    data_manager = trading_data_management.TradingDataManager()
    data_manager.populate_Sp500(datetime.date(2015, 4, 7))
