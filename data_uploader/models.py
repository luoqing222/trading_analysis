__author__ = 'Qing'

import peewee
import configparser
from peewee import *

#this table save the bar data from eod
db = MySQLDatabase(None)

class Bar1MinEodData(peewee.Model):
    symbol = peewee.CharField()
    transaction_date = peewee.DateField()
    transaction_time = peewee.DateTimeField()
    open_price = peewee.FloatField()
    high_price = peewee.FloatField()
    low_price = peewee.FloatField()
    close_price = peewee.FloatField()
    volume = peewee.BigIntegerField()
    exchange = peewee.CharField()

    class Meta:
        database = db
        primary_key = CompositeKey('transaction_time','symbol', 'exchange')
