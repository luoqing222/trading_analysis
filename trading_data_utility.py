__author__ = 'qingluo'

import BeautifulSoup
import urllib
import models
import datetime
import sets
import pandas as pd
from datetime import timedelta
import peewee
import re

# this class is used for the simple data utility functionality
#such as delete, update data tables
class TradingDataUtility:
    def __init__(self):
        pass

####################################################utility function for Sp500List in the model########################

    @staticmethod
    def create_sp500_list():
        ''' method to create the Sp500 list table. function has been tested
        :return: True means that table is just created, False means table already exists
        '''
        models.db.connect()
        if not models.Sp500List.table_exists():
            models.db.create_table(models.Sp500List)
            return True
        return False


    @staticmethod
    def add_symbol(date, symbol):
        ''' method to add the symbol in the Sp500List table.
        :param date: the time stamp when adding the symbol
        :param symbol: sp500 symbol
        :return:
        '''
        if not models.Sp500List.select().where(models.Sp500List.save_date==date, models.Sp500List.symbol==symbol).exists():
            models.Sp500List.create(save_date=date, symbol=symbol)

    @staticmethod
    def get_sp500_list(date):
        ''' methdo to return the sp500 list that has closest save_date ahead to date
        :param date: date
        :return: the sp500 list that has closest save_date ahead to date
        '''
        sp_list = []
        for sp in models.Sp500List.select(models.Sp500List.save_date).distinct():
            sp_list.append(sp.save_date)
        sp_list.sort()
        save_date = sp_list[-1]
        if sp_list[0] > date:
            save_date = sp_list[0]
        else:
            for i in range(0, len(sp_list) - 1):
                if sp_list[i] <= date < sp_list[i + 1]:
                    save_date = sp_list[i]

        sp500_list = models.Sp500List.select().where(models.Sp500List.save_date == save_date)
        result = []
        for it in sp500_list:
            result.append(it.symbol)
        return result


#functions to manage the strong stock table
    @staticmethod
    def create_strong_stock_table():
        models.db.create_table(models.StrongStock)


#function to manage the NYSEList table
#input
    def populate_NYSEList(self,file_name, update_date):
        models.db.connect()
        if not models.NYSEList.table_exists():
            models.db.create_table(models.NYSEList)

        with open(file_name,"r") as f:
            next(f)
            for line in f:
                [symbol, description] = re.split(r'\t', line, maxsplit=2)
                description = description[:-1]
                models.NYSEList.create(last_update_date=update_date, symbol= symbol, description=description)

#function to manage the NasdaqList table
    def populate_NasdaqList(self, file_name, update_date):
        models.db.connect()
        if not models.NasdaqList.table_exists():
            models.db.create_table(models.NasdaqList)

        with open(file_name,"r") as f:
            next(f)
            for line in f:
                [symbol, description] = re.split(r'\t', line, maxsplit=2)
                description = description[:-1]
                models.NasdaqList.create(last_update_date=update_date, symbol= symbol, description=description)

#function to manage the IndexList table
    def populate_IndexList(self, file_name, update_date):
        models.db.connect()
        if not models.IndexList.table_exists():
            models.db.create_table(models.IndexList)

        with open(file_name,"r") as f:
            next(f)
            for line in f:
                [symbol, description] = re.split(r'\t', line, maxsplit=2)
                description = description[:-1]
                models.IndexList.create(last_update_date=update_date, symbol= symbol, description=description)
















