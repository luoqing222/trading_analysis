__author__ = 'qingluo'

__author__ = 'qingluo'

import peewee
from peewee import *

db = MySQLDatabase('trading_data', user='root', passwd='uscusc')


class HolidayCalendar(peewee.Model):
    date = peewee.DateField()
    country_code = peewee.CharField()

    class Meta:
        database = db


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


