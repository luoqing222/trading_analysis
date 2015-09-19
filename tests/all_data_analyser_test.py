__author__ = 'Qing'

import sys,os
sys.path.append(os.path.realpath('..'))
from data_analyser import eoddata_analyser
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

    eod_data_analyser=eoddata_analyser.EodDataAnalyser(host, database, user, password)
    yahoo_data_analyser=yahoo_option_analyser.YahooOptionAnalyser(host, database, user, password)

    num_of_days_before_analysis=7
    filter_parameter=2.0

    start_date = datetime.datetime(year=2015, month=05, day=01)
    end_date = datetime.datetime.now()

    analysis_time = datetime.datetime(year=2015, month=9, day=18)
    result_table = yahoo_data_analyser.find_contracts_with_significant_volume_change(analysis_time, num_of_days_before_analysis,filter_parameter)

    folder = os.path.dirname(os.getcwd())+"/messages"
    with PdfPages(folder+"/"+"test1.pdf") as pdf:
        for symbol in result_table['underlying_stock']:
            option_fig = plt.figure(figsize=(16,6))
            ax = option_fig.add_subplot(1, 1, 1)
            yahoo_data_analyser.generate_option_sum_bar(start_date,end_date,symbol,ax)
            pdf.savefig()
            plt.close()

            stock_volume_fig = plt.figure(figsize=(16,6))
            ax = stock_volume_fig.add_subplot(1, 1, 1)
            eod_data_analyser.generate_stock_volume_bar(start_date, end_date, symbol,ax)
            pdf.savefig()
            plt.close()

            stock_plot_fig = plt.figure(figsize=(16,6))
            ax = stock_plot_fig.add_subplot(1, 1, 1)
            eod_data_analyser.generate_stock_closed_price_plot(start_date,end_date,symbol,ax, 'red')
            eod_data_analyser.generate_stock_weighted_average_price_plot(start_date,end_date,symbol,ax, 'blue')
            pdf.savefig()
            plt.close()

            stock_bar_fig = plt.figure(figsize=(16,6))
            ax = stock_bar_fig.add_subplot(1, 1, 1)
            eod_data_analyser.generate_stock_bar_volume_bar(start_date, end_date, symbol,ax)
            pdf.savefig()
            plt.close()




