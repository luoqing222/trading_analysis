__author__ = 'qingluo'

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

if __name__ == "__main__":

    start_time = time.time()
    start_date="2015/06/29"
    end_date="2015/06/30"
    start_date_object = datetime.datetime.strptime(start_date, '%Y/%m/%d')
    end_date_object = datetime.datetime.strptime(end_date, '%Y/%m/%d')
    trading_date_mapping = trading_date_utility.generate_previous_trading_date_dict(start_date_object,end_date_object,100)

    symbols = trading_data_utility.TradingDataUtility().get_sp500_list(end_date_object.date())
    days_array=[5,20,65]
    weight=[0.5,0.3,0.2]
    stock_num=10

    db = MySQLdb.connect(host=models.host,db=models.database, user=models.user, passwd=models.password)
    data_analyser = yahoo_data_analyser.YahooEquityDataAnalyser(db)
    temp_date_object=end_date_object.date()

    #write the header to historical_strong_stock.csv
    file = open("historical_strong_stock.csv", "w")
    file.write("date,")
    for index in range(0,stock_num):
        file.write("rank_"+str(index+1)+",")
    file.write("avg_return")
    file.write("\n")

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
    #print data_analyser.get_returns_between_two_days(symbols,start_date_object,end_date_object)
    db.close()
    file.close()
    print("--- %s seconds ---" % (time.time() - start_time))




