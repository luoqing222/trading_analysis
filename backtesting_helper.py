__author__ = 'qingluo'

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



if __name__ == "__main__":

    #first get the holiday in 2014
    #the rule to tell if a holiday is holiday is that no spy data on that day but is not weekend



    #start_date=



    #data_analyser = yahoo_data_analyser.YahooEquityDataAnalyser()
    #file_name = "sp500_daily_rsq_"+datetime.datetime.now().strftime('%m_%d_%Y')+".csv"
    #data_analyser.calculate_daily_rsq(symbol_list,index_list, None, 30, file_name)
    #send_email(file_name)

    # function to generate "constituents_wiki.csv"
    data_manager= trading_data_management.TradingDataManager()
    # start_date="2015/04/08"
    # end_date="2015/05/08"
    # dates=trading_date_utility.data_available_dates(start_date, end_date, "spy")
    # for date in dates:
    #     (year, week, weekday) = date.isocalendar()
    #     print str(weekday)

    data_manager.populate_historical_holiday()
    # data_manager.generate_constituents("constituents_wiki.csv")
    #data_manager.delete_holiday(2015,4,3,"US")
    #data_manager.add_holiday(2015,4,3,"US")
    #data_manager.populate_holiday()




