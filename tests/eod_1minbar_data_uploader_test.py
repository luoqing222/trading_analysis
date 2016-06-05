__author__ = 'Qing'

import logging
import configparser
import datetime
import sys,os
sys.path.append(os.path.realpath('..'))
from data_uploader import eod_1minbar_data_uploader

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    config_file = os.path.dirname(os.getcwd())+"/"+"option_data_management_setting.ini"
    config = configparser.ConfigParser()
    config.read(config_file)

    host = config.get("database", "host")
    database = config.get("database", "database")
    user = config.get("database", "user")
    password = config.get("database", "passwd")
    des_folder = config.get("csv","data_folder")
    data_uploader = eod_1minbar_data_uploader.Eod1MinBarDataUploader(host, database, user,password, des_folder)
    logging.basicConfig(filename='test.log', level=logging.INFO,filemode="w")
    logger.info("begin eod 1min bar data upload test")

    #running_time = datetime.datetime.now()
    running_time = datetime.datetime(year=2016, month=06, day=02)
    data_uploader.run(running_time)

    #running_time = datetime.datetime(year=2016, month=01, day=07)
    #data_uploader.run(running_time)

    #start_time = datetime.datetime(year = 2015, month = 9, day=12)
    #end_time = datetime.datetime(year =2015, month=9, day=19)
    #data_uploader.historical_weighted_avg_calc(start_time, end_time)
    #data_uploader.calculate_daily_weighted_average(host,database,user,password, running_time)
    #data_uploader.historical_run(start_time,end_time)
