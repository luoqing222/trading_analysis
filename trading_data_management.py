__author__ = 'qingluo'


import wiki_data_loader
import trading_data_utility
#this class is to data operation for database after data is loaded and parsed
class TradingDataManager:
    def __init__(self):
        pass

    def updateSp500(self):
        pass

    @staticmethod
    def populate_Sp500(date):
        data_loader = wiki_data_loader.WikiDataLoader()
        data_utility = trading_data_utility.TradingDataUtility()
        sp500_list = data_loader.get_latest_sp500()

        data_utility.create_sp500_list()

        for symbol in sp500_list:
            data_utility.add_symbol(date,symbol)
















