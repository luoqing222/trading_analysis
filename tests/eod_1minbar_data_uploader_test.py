__author__ = 'Qing'

import logging
import configparser
import datetime
import data_uploader

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    config_file = "../option_data_management_setting.ini"
    config = configparser.ConfigParser()
    config.read(config_file)

    host = config.get("database", "host")
    database = config.get("database", "database")
    user = config.get("database", "user")
    password = config.get("database", "passwd")
    des_folder = config.get("csv","data_folder")
    data_uploader = data_uploader.eod_1minbar_data_uploader(host, database, user,password, des_folder)
    logging.basicConfig(filename='test.log', level=logging.INFO,filemode="w")
    logger.info("begin eod 1min bar data upload test")

    #running_time = datetime.datetime.now()
    running_time = datetime.datetime(year=2015, month=8, day=28)
    data_uploader.run(running_time)
