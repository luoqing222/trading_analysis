__author__ = 'qingluo'

import yahoo_option_data_manager
import eoddata_data_manager
import time
import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

def create_options_bar_pdf():
    option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()
    #analysis_date = datetime.datetime.now()
    analysis_date = datetime.datetime(year=2015, month=7, day=13)
    stock_selected = []
    for num in range(0, 1):
        # running_date = analysis_date+datetime.timedelta(days = -(5-num))
        running_date = analysis_date
        date_window = []
        for day in range(1, 7):
            date_window.append(running_date + datetime.timedelta(days=-day))
        filter_parameter = 2.0;
        stock_selected = option_data_manager.filter_stock_by_option_volume(running_date, date_window, filter_parameter)

    start_date = datetime.datetime(year=2015, month=06, day=01)
    end_date = analysis_date

    num_of_sub_plot =3
    num_of_pdf_pages = len(stock_selected)/num_of_sub_plot+1

    with PdfPages('multi_page_pdf.pdf') as pdf:
        for page_num in range(0,num_of_pdf_pages):
            fig = plt.figure()
            for plot_num in range(0,num_of_sub_plot):
                index = page_num*num_of_sub_plot+plot_num
                if index < len(stock_selected):
                    underlying_stock = stock_selected[index][0]
                    expire_date = stock_selected[index][1]
                    ax = fig.add_subplot(num_of_sub_plot, 1, plot_num + 1)
                    option_data_manager.generate_option_sum_bar(start_date, end_date, underlying_stock, ax, expire_date)
            pdf.savefig()  # saves the current figure into a pdf page
            plt.close()

def compare_option_with_stock():
    option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()
    eod_data_manager = eoddata_data_manager.EodDataDataManager()
    #analysis_date = datetime.datetime.now()
    analysis_date = datetime.datetime(year=2015, month=7, day=13)
    stock_selected = []
    for num in range(0, 1):
        # running_date = analysis_date+datetime.timedelta(days = -(5-num))
        running_date = analysis_date
        date_window = []
        for day in range(1, 7):
            date_window.append(running_date + datetime.timedelta(days=-day))
        filter_parameter = 2.0;
        stock_selected = option_data_manager.filter_stock_by_option_volume(running_date, date_window, filter_parameter)

    start_date = datetime.datetime(year=2015, month=06, day=01)
    end_date = analysis_date

    #num_of_sub_plot =2
    #num_of_pdf_pages = len(stock_selected)

    with PdfPages('comparison_pdf.pdf') as pdf:
        for underlying_stock,expire_date in stock_selected:
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
    option_data_manager.generate_option_sum_bar(start_date, end_date, underlying_stock, ax, end_date)
    ax = fig.add_subplot(2,1,2)
    eod_data_manager.generate_stock_closed_price_plot(start_date, end_date, underlying_stock, ax)
    plt.show()

if __name__ == "__main__":

    start_time = time.time()
    #option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()
    # option_data_manager.daily_run()
    # option_data_manager.save_historical_data()
    #create_options_bar_pdf()
    #compare_option_with_stock()

    start_date = datetime.datetime(year=2015, month=06, day=01)
    end_date = datetime.datetime(year=2015, month=07, day=13)
    symbol = "AMCX"
    single_compare_option_with_stock(start_date,end_date, symbol)

    print("--- %s seconds ---" % (time.time() - start_time))
