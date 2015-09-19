__author__ = 'Qing'

import sys,os
sys.path.append(os.path.realpath('..'))
from data_analyser import yahoo_option_analyser
import configparser
import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

if __name__ == "__main__":

    config_file = os.path.dirname(os.getcwd())+"/"+"option_data_management_setting.ini"
    config = configparser.ConfigParser()
    config.read(config_file)

    host = config.get("database", "host")
    database = config.get("database", "database")
    user = config.get("database", "user")
    password = config.get("database", "passwd")

    yahoo_data_analyser=yahoo_option_analyser.YahooOptionAnalyser(host, database, user, password)

    start_date = datetime.datetime(year=2015, month=05, day=01)
    end_date = datetime.datetime.now()

    test_date1 = datetime.datetime(year=2015, month=6, day=28)
    test_date2 = datetime.datetime(year=2015, month=9, day=15)
    fig = plt.figure(figsize=(16,6))

    ax = fig.add_subplot(1, 1, 1)
    symbol = 'ZOES'
    yahoo_data_analyser.generate_option_sum_bar(start_date,end_date,symbol,ax)

    plt.show()