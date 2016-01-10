__author__ = 'Qing'

import logging
import sys,os
import configparser
sys.path.append(os.path.realpath('..'))
from data_uploader import index_component_data_uploader

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    config_file = os.path.dirname(os.getcwd())+"/"+"option_data_management_setting.ini"
    config = configparser.ConfigParser()
    config.read(config_file)

    host = config.get("database", "host")
    database = config.get("database", "database")
    user = config.get("database", "user")
    password = config.get("database", "passwd")
    data_uploader = index_component_data_uploader.IndexComponentDataUploader(host, database, user,password)

    file_name='xli.csv'
    file_folder = "C:/dev/temp"
    data_uploader.run(file_name,file_folder)

