__author__ = 'Qing'

import sys, os
import configparser
import datetime
sys.path.append(os.path.realpath('..'))
from data_collectors import seekingalpha_idea_data_collector
import trading_data_utility_by_sql

if __name__ == "__main__":
    config_file = os.path.dirname(os.getcwd())+"/"+"option_data_management_setting.ini"
    config = configparser.ConfigParser()
    config.read(config_file)
    driver_location = config.get("driver", "chrome_driver")
    host = config.get("database", "host")
    database = config.get("database", "database")
    user = config.get("database", "user")
    password = config.get("database", "passwd")
    des_folder = config.get("csv","data_folder")

    data_collector = seekingalpha_idea_data_collector.SeekingAlphaIdeaDataCollector(driver_location=driver_location)
    running_time= datetime.datetime.now()

    nasdaq_list = trading_data_utility_by_sql.TradingDataUtilityBySQL(host,database, user, password).get_nasdaq_list(running_time)
    nyse_list= trading_data_utility_by_sql.TradingDataUtilityBySQL(host,database, user, password).get_nyse_list(running_time)

    stock_list = list(set(nyse_list + nyse_list))
    data_collector.run(running_time,des_folder, stock_list)

