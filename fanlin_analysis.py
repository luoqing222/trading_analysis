__author__ = 'Qing'

import wiki_data_loader
import trading_data_manager
import datetime
import trading_data_utility
import trading_date_utility
import yahoo_data_analyser
import time
import numpy
import models
import MySQLdb
import sys
import os
from peewee import fn
import emailprocessing
import yahoo_equity_data_loader

def get_messages_folder():
    current_folder = os.getcwd()
    message_folder = current_folder + "/" + "messages"
    #message_folder = current_folder + "/" + "temp"
    return message_folder

def generate_rsq_file_name(date_object):
    return "sp500_daily_rsq_" + date_object.strftime('%m_%d_%Y') + ".csv"

def generate_strong_stock_file_name(date_object):
    return "historical_strong_stock_"+date_object.strftime('%m_%d_%Y')+".csv"

def send_email(file_name, mail_list, folder):
    gm = emailprocessing.Gmail('raki1978wmc6731@gmail.com', 'Fapkc1897Fapkc')

    #subject = "analysis report on " + datetime.datetime.now().strftime('%m_%d_%Y')
    subject = file_name
    message = "This is a test message"
    # to = "luoqing222@gmail.com"

    # gm.send_message(subject, message, 'luoqing222@gmail.com')
    for to in mail_list:
        gm.send_text_attachment(subject, to, file_name, folder)

def calculate_strong_stock(start_date_object, end_date_object):
    '''
    :param start_date_object: starting_date in the running
    :param end_date_object: end_date in the running
    :return:
    '''
    trading_date_mapping = trading_date_utility.generate_previous_trading_date_dict(start_date_object.date(),end_date_object.date(),120)
    symbols = trading_data_utility.TradingDataUtility().get_sp500_list(end_date_object.date())
    days_array=[5,20,65]
    weight=[0.5,0.3,0.2]
    stock_num=10
    db = MySQLdb.connect(host=models.host,db=models.database, user=models.user, passwd=models.password)
    data_analyser = yahoo_data_analyser.YahooEquityDataAnalyser(db)
    #temp_date_object=end_date_object.date()
    temp_date_object=end_date_object

    message_folder = get_messages_folder()
    if not os.path.exists(message_folder):
        os.makedirs(message_folder)

    #write the header to historical_strong_stock.csv
    file_name = generate_strong_stock_file_name(end_date_object)
    file = open(message_folder + "/" + file_name, "w")
    file.write("date,")
    for index in range(0,stock_num):
        file.write("rank_"+str(index+1)+",")
    file.write("avg_return")
    file.write("\n")

    temp_date_object = temp_date_object.date()
    while temp_date_object>start_date_object.date():
        print "running calculation on ", temp_date_object
        last_week_date=trading_date_utility.previous_n_trading_days(temp_date_object,5,trading_date_mapping)
        strong_stock= data_analyser.get_n_days_returns_rank_by_sql(symbols, last_week_date, trading_date_mapping, days_array,weight,stock_num)
        #print last_week_date," strong stock is ", strong_stock
        return_start_date=trading_date_utility.previous_n_trading_days(temp_date_object,4,trading_date_mapping)

        avg_return= data_analyser.get_average_between_two_days(strong_stock,return_start_date,temp_date_object)
        file.write(temp_date_object.strftime('%Y-%m-%d')+",")
        temp_date_object = trading_date_utility.previous_n_trading_days(temp_date_object,1,trading_date_mapping)
        for index in range(0,stock_num):
            file.write(strong_stock[index]+",")
        file.write(str(avg_return))
        file.write("\n")
    db.close()
    file.close()

def calculate_sp500_rsq(running_date_object):
    # find the tick that in the HistoricalPrice
    query = models.HistoricalPrice.select(models.HistoricalPrice.symbol,
                                         fn.Max(models.HistoricalPrice.transaction_date).
                                         alias('recent_date')).group_by(models.HistoricalPrice.symbol)

    symbol_most_recent_date = {}
    #mapping from symbol to most recent date is saved in dictionary symbol_most_recent_date
    #including the index fund price
    for item in query:
       symbol_most_recent_date[item.symbol] = item.recent_date

    # get the SP500 list with most recent save date
    query = models.Sp500List.select(fn.Max(models.Sp500List.save_date).alias('recent_date'))
    for item in query:
        SP500_recent_date = item.recent_date

    # item in the query is latest sp500 tick
    symbol_list = []
    query = models.Sp500List.select().where(models.Sp500List.save_date == SP500_recent_date)
    the_data_loader = yahoo_equity_data_loader.YahooEquityDataLoader()

    for item in query:
        symbol = item.symbol
        the_data_loader.save_trading_data(symbol, symbol_most_recent_date)
        symbol_list.append(symbol)

    # do the same thing for index fund
    query = models.IndexSymbol.select()
    index_list = []
    for item in query:
        symbol = item.symbol
        the_data_loader.save_trading_data(symbol, symbol_most_recent_date)
        index_list.append(symbol)

    # make the directory for the messages to monitor
    message_folder = get_messages_folder()
    if not os.path.exists(message_folder):
        os.makedirs(message_folder)

    db = MySQLdb.connect(host=models.host, db=models.database, user=models.user, passwd=models.password)
    data_analyser = yahoo_data_analyser.YahooEquityDataAnalyser(db)
    file_name = generate_rsq_file_name(running_date_object)
    running_date_object = running_date_object.date()
    data_analyser.calculate_daily_rsq(symbol_list, index_list, running_date_object, 30, message_folder + "/" + file_name)
    db.close()

def historical_rsq_run(start_date, end_date):
    # find the tick that in the HistoricalPrice
    start_date_object = datetime.datetime.strptime(start_date, '%Y/%m/%d')
    end_date_object = datetime.datetime.strptime(end_date, '%Y/%m/%d')
    while start_date_object<=end_date_object:
        if not trading_date_utility.is_trading_day(start_date_object,"US"):
            pass
        else:
            calculate_sp500_rsq(start_date_object)
        start_date_object=start_date_object+datetime.timedelta(days=1)

if __name__ == "__main__":
    #start_date="2015/05/09"
    #end_date="2015/11/27"
    #historical_rsq_run(start_date,end_date)
    # check if the day that is not trading day, stop running
    current_time = datetime.datetime.now()
    #current_time = datetime.datetime(2015,9,4,0,0,0)
    if not trading_date_utility.is_trading_day(current_time, "US"):
        sys.exit(0)

    calculate_sp500_rsq(current_time)
    calculate_strong_stock(current_time+datetime.timedelta(days = -1),current_time)
    mail_list = ["luoqing222@gmail.com", "fanlinzhu@yahoo.com"]
    send_email(generate_rsq_file_name(current_time), mail_list,get_messages_folder())
    send_email(generate_strong_stock_file_name(current_time), mail_list,get_messages_folder())



