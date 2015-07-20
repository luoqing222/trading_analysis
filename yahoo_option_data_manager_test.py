__author__ = 'qingluo'

import yahoo_option_data_manager
import eoddata_data_manager
import trading_date_utility
import time
import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os

def find_contracts_with_significant_volume_change(analysis_date, num_of_days_before_analysis,filter_parameter):
    if not trading_date_utility.is_trading_day(analysis_date,"US"):
        return None
    option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()

    running_date = analysis_date
    date_window = []
    for day in range(1, num_of_days_before_analysis):
        date_window.append(running_date + datetime.timedelta(days=-day))
    result_table = option_data_manager.filter_stock_by_option_volume(running_date, date_window, filter_parameter)
    return result_table

def create_options_bar_pdf(start_date,end_date, contract_list, folder, file_name):
    option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()

    num_of_sub_plot =3
    num_of_pdf_pages = len(contract_list)/num_of_sub_plot+1

    with PdfPages(folder+"/"+file_name) as pdf:
        for page_num in range(0,num_of_pdf_pages):
            fig = plt.figure()
            for plot_num in range(0,num_of_sub_plot):
                index = page_num*num_of_sub_plot+plot_num
                if index < len(contract_list):
                    print index
                    print contract_list[index]
                    [underlying_stock, expire_date, option_type, strike_price]=option_data_manager.decompose_option_contract(contract_list[index])
                    ax = fig.add_subplot(num_of_sub_plot, 1, plot_num + 1)
                    option_data_manager.generate_option_sum_bar(start_date, end_date, underlying_stock, ax, expire_date)
            pdf.savefig()  # saves the current figure into a pdf page
            plt.close()

def create_option_stock_comparison(start_date,end_date,contract_list,folder,file_name):
    option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()
    eod_data_manager = eoddata_data_manager.EodDataDataManager()

    with PdfPages(folder+"/"+file_name) as pdf:
        for contract in contract_list:
            [underlying_stock, expire_date, option_type, strike_price]=option_data_manager.decompose_option_contract(contract)
            fig = plt.figure()
            ax = fig.add_subplot(2, 1, 1)
            option_data_manager.generate_option_sum_bar(start_date, end_date, underlying_stock, ax, expire_date)
            ax = fig.add_subplot(2,1,2)
            eod_data_manager.generate_stock_closed_price_plot(start_date, end_date, underlying_stock, ax)
            pdf.savefig()  # saves the current figure into a pdf page
            plt.close()

def single_compare_option_with_stock(start_date,end_date, underlying_stock):
    option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()
    eod_data_manager = eoddata_data_manager.EodDataDataManager()

    fig = plt.figure()
    ax = fig.add_subplot(2, 1, 1)
    option_data_manager.generate_option_sum_bar(start_date, end_date, underlying_stock, ax, end_date.strftime('%Y_%m_%d'))
    ax = fig.add_subplot(2,1,2)
    eod_data_manager.generate_stock_closed_price_plot(start_date, end_date, underlying_stock, ax)

def save_abnormal_options(result_table):
    option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()
    option_data_manager.save_abnormal_options(result_table)


if __name__ == "__main__":

    start_time = time.time()
    #option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()
    # option_data_manager.daily_run()
    # option_data_manager.save_historical_data()

    num_of_days_before_analysis=7
    filter_parameter=2.0
    start_date = datetime.datetime(year=2015, month=6,day=1)
    end_date=datetime.datetime(year=2015,month=7,day=17)

    #print option_data_manager.decompose_option_contract("NBCXD150821P00075000")
    for i in range(0,20):
    #for i in range(0,1):
        analysis_date = datetime.datetime(year=2015,month=7,day=17)+datetime.timedelta(days = -i)
        if trading_date_utility.is_trading_day(analysis_date,"US"):
            result_table = find_contracts_with_significant_volume_change(analysis_date, num_of_days_before_analysis,filter_parameter)
            if result_table is not None:
                save_abnormal_options(result_table)
                contract_list = [x for x in result_table.contract]
                if contract_list is not None:
                    current_folder = os.getcwd()
                    message_folder = current_folder + "/" + "messages"
                    file_name = "contract_" + analysis_date.strftime('%m_%d_%Y') + ".pdf"
                    #print contract_list
                    create_options_bar_pdf(start_date,end_date,contract_list,message_folder,file_name)
                    file_name = "contrast_" + analysis_date.strftime('%m_%d_%Y') + ".pdf"
                    create_option_stock_comparison(start_date,end_date,contract_list,message_folder,file_name)

    #symbol = "AMCX"
    #single_compare_option_with_stock(start_date,end_date, symbol)
    #plt.show()

    print("--- %s seconds ---" % (time.time() - start_time))
