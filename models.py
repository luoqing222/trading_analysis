__author__ = 'qingluo'

__author__ = 'qingluo'

import peewee
import configparser
from peewee import *


Config = configparser.ConfigParser()
Config.read("option_data_management_setting.ini")
host = Config.get("database", "host")
database = Config.get("database", "database")
user = Config.get("database", "user")
password = Config.get("database", "passwd")

db = MySQLDatabase(host=host, database=database, user=user, passwd=password)


class HolidayCalendar(peewee.Model):
    date = peewee.DateField()
    country_code = peewee.CharField()

    class Meta:
        database = db
        primary_key = CompositeKey('date', 'country_code')


# class Sp500Symbol(peewee.Model):
# id = peewee.PrimaryKeyField()
#     symbol = peewee.CharField()
#     name = peewee.CharField()
#     sector = peewee.CharField()
#     save_date = peewee.DateField()
#
#     class Meta:
#         database = db

#correpond to table NYSEList in the database
class NYSEList(peewee.Model):
    symbol = peewee.CharField()
    last_update_date = peewee.DateField()
    description = peewee.CharField()

    class Meta:
        database = db
        primary_key = CompositeKey('last_update_date', 'symbol')


#correponds to table NasdaqList in the database
class NasdaqList(peewee.Model):
    symbol = peewee.CharField()
    last_update_date = peewee.DateField()
    description = peewee.CharField()

    class Meta:
        database = db
        primary_key = CompositeKey('last_update_date', 'symbol')


#correspond to table IndexList in the database
class IndexList(peewee.Model):
    symbol = peewee.CharField()
    last_update_date = peewee.DateField()
    description = peewee.CharField()

    class Meta:
        database = db
        primary_key = CompositeKey('last_update_date', 'symbol')


# this table save the time series of sp500 list
class Sp500List(peewee.Model):
    symbol = peewee.CharField()
    save_date = peewee.DateField()

    class Meta:
        database = db
        primary_key = CompositeKey('symbol', 'save_date')


#this table save the index symbol
class IndexSymbol(peewee.Model):
    id = peewee.PrimaryKeyField()
    symbol = peewee.CharField()
    name = peewee.CharField()

    class Meta:
        database = db


#this table save the historical price of sp500
class HistoricalPrice(peewee.Model):
    symbol = peewee.CharField()
    transaction_date = peewee.DateField()
    open = peewee.FloatField()
    high = peewee.FloatField()
    close = peewee.FloatField()
    adjust_close = peewee.FloatField()
    volume = peewee.BigIntegerField()

    class Meta:
        database = db
        order_by = ('symbol', 'transaction_date',)


#this table save the strong stock per day per Fanlin's definition
#The strong stock is defined as the weighted sum of the ranking in one week, 4weeks and 13weeks return
class StrongStock(peewee.Model):
    symbol = peewee.CharField()
    calculation_date = peewee.DateField()
    rank = peewee.IntegerField()

    class Meta:
        database = db
        primary_key = CompositeKey('calculation_date', 'rank')
        order_by = ('calculation_date', 'rank')

#this table save the option per day. data source is finance.yahoo.com
class YahooOption(peewee.Model):
    contract = peewee.CharField()
    transaction_date = peewee.DateField()
    underlying_stock = peewee.CharField()
    expire_date = peewee.DateField()
    #strike price is 1000 times the actual strike price so it can be saved as integer
    strike_price = peewee.IntegerField()
    option_type = peewee.CharField()
    last = peewee.FloatField()
    bid = peewee.FloatField()
    ask = peewee.FloatField()
    price_change = peewee.FloatField()
    pct_change = peewee.FloatField()
    volume = peewee.IntegerField()
    open_interest = peewee.IntegerField()
    implied_vol = peewee.FloatField()

    class Meta:
        database = db
        primary_key = CompositeKey('transaction_date','underlying_stock','expire_date','strike_price','option_type')

#this table save the equity information. data source is eod
class EodEquity(peewee.Model):
    symbol = peewee.CharField()
    transaction_date = peewee.DateField()
    open_price = peewee.FloatField()
    high_price = peewee.FloatField()
    low_price = peewee.FloatField()
    close_price = peewee.FloatField()
    volume = peewee.BigIntegerField()
    exchange = peewee.CharField()

    class Meta:
        database = db
        primary_key = CompositeKey('transaction_date','symbol', 'exchange')

#this table save the option per day. data source is finance.yahoo.com
class EodOption(peewee.Model):
    contract = peewee.CharField()
    transaction_date = peewee.DateField()
    underlying_stock = peewee.CharField()
    expire_date = peewee.DateField()
    strike_price = peewee.IntegerField()
    option_type = peewee.CharField()
    open_price = peewee.FloatField()
    low_price = peewee.FloatField()
    high_price = peewee.FloatField()
    close_price = peewee.FloatField()
    volume = peewee.IntegerField()
    open_interest = peewee.IntegerField()

    class Meta:
        database = db
        primary_key = CompositeKey('transaction_date','underlying_stock','expire_date','strike_price','option_type')




