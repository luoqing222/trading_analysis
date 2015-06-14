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


    @staticmethod
    def delete_holiday(year, month, day, country):
        models.db.connect()

        holiday = datetime.datetime(year, month, day)
        query = models.HolidayCalendar.delete().where(models.HolidayCalendar.date == holiday,
                                                      models.HolidayCalendar.country_code == country)
        query.execute()


    @staticmethod
    def add_holiday(year, month, day, country):
        models.db.connect()
        if not models.HolidayCalendar.table_exists():
            models.db.create_table(models.HolidayCalendar)

        holiday = datetime.datetime(year, month, day)
        models.HolidayCalendar.create(date=holiday, country_code=country)


    #function to populate the holiday into database table
    @staticmethod
    def populate_holiday():
        models.HolidayCalendar.drop_table()
        models.db.create_table(models.HolidayCalendar)
        TradingDataManager.add_holiday(2015, 4, 3, "US")
        TradingDataManager.add_holiday(2015, 5, 25, "US")
        TradingDataManager.add_holiday(2015, 7, 3, "US")
        TradingDataManager.add_holiday(2015, 9, 7, "US")
        TradingDataManager.add_holiday(2015, 11, 26, "US")
        TradingDataManager.add_holiday(2015, 11, 27, "US")
        TradingDataManager.add_holiday(2015, 12, 25, "US")

    #function to populate the historical holiday into database table
    #rule: is the dates has SPY data, we consider it as trading date, spy data staring from 1993/01/29
    @staticmethod
    def populate_historical_holiday():
        trading_dates = sets.Set([])
        start_date = "1983/01/01"
        update_date = "2015/05/09"
        symbol = "spy"
        data_record = models.HistoricalPrice.select().where((models.HistoricalPrice.symbol == symbol)
                                                            & (models.HistoricalPrice.transaction_date >= start_date)
                                                            & (
                                                                models.HistoricalPrice.transaction_date <= update_date)).order_by(
            models.HistoricalPrice.transaction_date)
        #print data_record.count()
        for i in range(0, data_record.count()):
            trading_dates.add(data_record[i].transaction_date.toordinal())
        for date in range(min(trading_dates), max(trading_dates)):
            if date not in trading_dates:
                nonordinal_date = datetime.date.fromordinal(date)
                (year, week, weekday) = nonordinal_date.isocalendar()
                if weekday < 6:
                    print nonordinal_date.year, nonordinal_date.month, nonordinal_date.day
                    try:
                        TradingDataManager.add_holiday(nonordinal_date.year, nonordinal_date.month, nonordinal_date.day,
                                                       "US")
                    except peewee.IntegrityError:
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















