__author__ = 'qingluo'

import emailprocessing
import yahoo_data_loader
import yahoo_data_analyser
import datetime
import models
import os
import sys
from peewee import fn
import pandas as pd
import urllib
import BeautifulSoup
import re
import dictionary_ids
import trading_data_management
import trading_date_utility
import wiki_data_loader


if __name__ == "__main__":

    # find the tick that in the HistoricalPrice
    query = models.HistoricalPrice.select(models.HistoricalPrice.symbol,
                                          fn.Max(models.HistoricalPrice.transaction_date).
                                          alias('recent_date')).group_by(models.HistoricalPrice.symbol)

    symbol_most_recent_date = {}
    # mapping from symbol to most recent date is saved in dictionary symbol_most_recent_date
    # including the index fund price
    for item in query:
        symbol_most_recent_date[item.symbol] = item.recent_date

    # get the SP500 list with most recent save date
    query = models.Sp500Symbol.select(fn.Max(models.Sp500Symbol.save_date).alias('recent_date'))
    for item in query:
        SP500_recent_date = item.recent_date

    # item in the query is latest sp500 tick
    symbol_list= []
    query = models.Sp500Symbol.select().where(models.Sp500Symbol.save_date == SP500_recent_date)
    for item in query:
        symbol = item.symbol
        #save_trading_data(symbol, symbol_most_recent_date)
        symbol_list.append(symbol)

    #do the same thing for index fund
    query = models.IndexSymbol.select()
    index_list= []
    for item in query:
        symbol = item.symbol
        #save_trading_data(symbol, symbol_most_recent_date)
        index_list.append(symbol)

    start_date="2013/05/08"
    end_date="2014/05/08"
    data_analyser = yahoo_data_analyser.YahooEquityDataAnalyser()
    file_name = "sp500_historical_rsq.csv"
    data_analyser.calculate_historical_rsq(symbol_list,index_list,start_date,end_date, 30, file_name)







