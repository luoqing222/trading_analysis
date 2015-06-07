__author__ = 'qingluo'

__author__ = 'qingluo'

import peewee
import configparser
from peewee import *


Config = configparser.ConfigParser()
Config.read("database_setting.ini")
host = Config.get("database","host")
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


class Sp500Symbol(peewee.Model):
    id = peewee.PrimaryKeyField()
    symbol = peewee.CharField()
    name = peewee.CharField()
    sector = peewee.CharField()
    save_date = peewee.DateField()

    class Meta:
        database = db

#correpond to table NYSEList in the database
class NYSEList(peewee.Model):
    symbol=peewee.CharField()
    last_update_date=peewee.DateField()
    description=peewee.CharField()

    class Meta:
        database = db
        primary_key = CompositeKey('last_update_date', 'symbol')

#correponds to table NasdaqList in the database
class NasdaqList(peewee.Model):
    symbol=peewee.CharField()
    last_update_date=peewee.DateField()
    description=peewee.CharField()

    class Meta:
        database = db
        primary_key = CompositeKey('last_update_date', 'symbol')


#correspond to table IndexList in the database
class IndexList(peewee.Model):
    symbol=peewee.CharField()
    last_update_date=peewee.DateField()
    description=peewee.CharField()

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

# class OptionPrice(peewee.Model):
#     underlying_stock = peewee.CharField()
#     transaction_date = peewee.DateField()
#     expire_date = peewee.DateField()
#     #strike price is 100 times the actual strike price so it can be saved as integer
#     strike_price = peewee.IntegerField()
#     #type is the option type, 0 for put and 1 for call
#     type = peewee.IntegerField()
#
#
#     class Meta:
#         database = db
#         primary_key= CompositeKey('underlying_stock', 'transaction_date', 'expire_date', 'strike_price', 'type')



