__author__ = 'Qing'

import sys,os
sys.path.append(os.path.realpath('..'))
from data_analyser import eoddata_analyser
import configparser
import datetime
import matplotlib.pyplot as plt

if __name__ == "__main__":
    config_file = os.path.dirname(os.getcwd())+"/"+"option_data_management_setting.ini"
    config = configparser.ConfigParser()
    config.read(config_file)

    host = config.get("database", "host")
    database = config.get("database", "database")
    user = config.get("database", "user")
    password = config.get("database", "passwd")

    eod_data_analyser=eoddata_analyser.EodDataAnalyser(host, database, user, password)

    start_date = datetime.datetime(year=2015, month=05, day=01)
    end_date = datetime.datetime.now()
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    eod_data_analyser.generate_stock_closed_price_plot(start_date,end_date,"ADPT",ax, 'red')
    eod_data_analyser.generate_stock_weighted_average_price_plot(start_date,end_date,"ADPT",ax, 'blue')
    plt.show()

