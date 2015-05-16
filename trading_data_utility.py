__author__ = 'qingluo'

import BeautifulSoup
import urllib
import models
import datetime
import sets
import pandas as pd
from datetime import timedelta
import peewee


#this class is used for the simple data utility functionality
#such as delete, update data tables
class TradingDataUtility:
    def __init__(self):
        pass


    @staticmethod
    def delete_holiday(year, month, day,country):
        models.db.connect()

        holiday = datetime.datetime(year, month, day)
        query= models.HolidayCalendar.delete().where(models.HolidayCalendar.date== holiday,
                                                     models.HolidayCalendar.country_code==country)
        query.execute()


    @staticmethod
    def add_holiday(year, month, day, country):
        models.db.connect()
        if not models.HolidayCalendar.table_exists():
            models.db.create_table(models.HolidayCalendar)

        holiday = datetime.datetime(year, month, day)
        models.HolidayCalendar.create(date=holiday, country_code= country)


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
        trading_dates=sets.Set([])
        start_date="1983/01/01"
        update_date="2015/05/09"
        symbol="spy"
        data_record = models.HistoricalPrice.select().where((models.HistoricalPrice.symbol == symbol)
                                        & (models.HistoricalPrice.transaction_date >= start_date)
        & (models.HistoricalPrice.transaction_date <= update_date)).order_by(models.HistoricalPrice.transaction_date)
        #print data_record.count()
        for i in range(0,data_record.count()):
            trading_dates.add(data_record[i].transaction_date.toordinal())
        for date in range(min(trading_dates),max(trading_dates)):
            if date not in trading_dates:
                nonordinal_date= datetime.date.fromordinal(date)
                (year, week, weekday) = nonordinal_date.isocalendar()
                if weekday < 6:
                    print nonordinal_date.year, nonordinal_date.month, nonordinal_date.day
                    try:
                        TradingDataManager.add_holiday(nonordinal_date.year, nonordinal_date.month, nonordinal_date.day, "US")
                    except peewee.IntegrityError:
                        pass

#function to run query on database table Sp500List
    @staticmethod
    def create_sp500_list():
        models.db.create_table(models.Sp500List)

    @staticmethod
    def add_symbol(date,symbol):
        models.Sp500List.create(save_date= date,symbol= symbol)










