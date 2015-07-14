__author__ = 'qingluo'

import yahoo_option_data_manager
import time
import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

if __name__ == "__main__":

    start_time = time.time()

    option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()
    # option_data_manager.daily_run()
    # option_data_manager.save_historical_data()

    analysis_date = datetime.datetime.now()
    #analysis_date = datetime.datetime(year=2015, month=07, day=10)
    stock_selected = []
    for num in range(0, 1):
        # running_date = analysis_date+datetime.timedelta(days = -(5-num))
        running_date = analysis_date
        date_window = []
        for day in range(1, 7):
            date_window.append(running_date + datetime.timedelta(days=-day))
        filter_parameter = 2.0;
        stock_selected = option_data_manager.filter_stock_by_option_volume(running_date, date_window, filter_parameter)

    start_date = datetime.datetime(year=2015, month=05, day=01)
    end_date = analysis_date
    #end_date = datetime.datetime(year=2015, month=07, day=10)
    #underlying_stocks = []
    #for underlying_stock, expire_date in stock_selected:
    #    underlying_stocks.append(underlying_stock)
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


    #fig = plt.figure()
    #for index, underlying_stock in enumerate(underlying_stocks):
    #    ax = fig.add_subplot(len(underlying_stocks), 1, index + 1)
    #    option_data_manager.generate_option_sum_bar(start_date, end_date, underlying_stock, ax)
    #plt.subplots_adjust(left=0.125, right=0.9, bottom=0.1, top=0.9, wspace=0.2, hspace=0.5)
    #plt.show()

    print("--- %s seconds ---" % (time.time() - start_time))
