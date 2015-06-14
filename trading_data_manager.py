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
        ''' function to update sp500list. has been tested
        :param date: function to update Sp500List in the daily run
        :return: None means no update in Sp500List, otherwise return the new added symbols
        '''
        try:
            sp500_wiki=self.data_loader.get_latest_sp500()
            if self.data_utility.create_sp500_list():
                for symbol in sp500_wiki:
                    self.data_utility.add_symbol(date,symbol)
                return None

            sp500_in_table= self.data_utility.get_sp500_list(date)

            if set(sp500_wiki) ==set(sp500_in_table):
                return None
            else:
                diff= set(sp500_in_table)-set(sp500_wiki)
                #something might be wrong in the wiki page parsing, so just return the difference
                #and no need to update the Sp500List
                if(len(diff)>10):
                    return diff
                #update the sp500list table using the wiki data
                else:
                    for symbol in sp500_wiki:
                        self.data_utility.add_symbol(date,symbol)
                    return diff
        except:
            return None

    #function to populate the NYSEList table
    def populate_NYSEList(self, file_name, date):
        self.data_utility.populate_NYSEList(file_name,date)

    def populate_NasdaqList(self, file_name, date):
        self.data_utility.populate_NasdaqList(file_name,date)

    def populate_IndexList(self, file_name, date):
        self.data_utility.populate_IndexList(file_name,date)























