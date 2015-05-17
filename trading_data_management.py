__author__ = 'qingluo'


import wiki_data_loader
import trading_data_utility
#this class is to data operation for database after data is loaded and parsed
class TradingDataManager:
    def __init__(self):
        self.data_loader= wiki_data_loader.WikiDataLoader()
        self.data_utility= trading_data_utility.TradingDataUtility()
        pass

    #function to check and update sp500 list in daily run
    def updateSp500(self):

        pass


    def populate_Sp500(self, date):
        sp500_list = self.data_loader.get_latest_sp500()
        self.data_utility.create_sp500_list()

        for symbol in sp500_list:
            self.data_utility.add_symbol(date,symbol)
















