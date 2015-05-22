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
    def updateSp500(self, date):
        sp500_wiki=self.data_loader.get_latest_sp500()
        sp500_in_table= self.data_utility.get_sp500_list(date)
        if set(sp500_wiki) ==set(sp500_in_table):
            return None
        else:
            diff= set(sp500_in_table)-set(sp500_wiki)
            #something might be wrong in the wiki page parsing, so just return the difference to the main
            if(len(diff)>10):
                return diff
            #just update the sp500list table using the wiki data
            else:
                for symbol in sp500_wiki:
                    self.data_utility.add_symbol(date,symbol)
                return diff

    #function to populate sp500 list after when initialize the table
    def populate_Sp500(self, date):
        sp500_list = self.data_loader.get_latest_sp500()
        self.data_utility.create_sp500_list()

        for symbol in sp500_list:
            self.data_utility.add_symbol(date,symbol)
















