__author__ = 'Qing'

import os
import configparser
from data_collectors import google_news_data_collector
import trading_data_utility_by_sql
import datetime

if __name__ == "__main__":

    config_file = os.path.dirname(os.getcwd())+"/"+"option_data_management_setting.ini"
    config = configparser.ConfigParser()
    config.read(config_file)

    des_folder = config.get("csv","data_folder")
    host = config.get("database", "host")
    database = config.get("database", "database")
    user = config.get("database", "user")
    password = config.get("database", "passwd")
    nyse_list= ['KO']
    des_folder = config.get("csv","data_folder")

    running_time= datetime.datetime.now()
    #nasdaq_list = trading_data_utility_by_sql.TradingDataUtilityBySQL(host,database, user, password).get_nasdaq_list(running_time)
    nasdaq_list = ['FB']
    data_collector = google_news_data_collector.GoogleNewsDataCollector(driver_location=des_folder,
                                                                        nyse_list=nyse_list, nasdaq_list=nasdaq_list)

    data_collector.run(running_time,des_folder)


