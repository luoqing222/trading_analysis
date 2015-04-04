__author__ = 'qingluo'

__author__ = 'qingluo'

import peewee
from peewee import *

db = MySQLDatabase('trading_data', user='root', passwd='uscusc')
#db = MySQLDatabase('trading_data', user='root', passwd='0307linsanlinqi)#)&')



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


class IndexSymbol(peewee.Model):
    id = peewee.PrimaryKeyField()
    symbol = peewee.CharField()
    name = peewee.CharField()

    class Meta:
        database = db


#

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



