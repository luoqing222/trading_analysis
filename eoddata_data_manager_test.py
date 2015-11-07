__author__ = 'qingluo'

import eoddata_data_manager
import time
import os
import datetime
import matplotlib.pyplot as plt

if __name__ == "__main__":

    start_time = time.time()

    eod_data_manager=eoddata_data_manager.EodDataDataManager()
    running_time = datetime.datetime(year=2015, month=11, day=04)
    eod_data_manager.run(running_time)

    # start_date = datetime.datetime(year=2015, month=05, day=01)
    # end_date = datetime.datetime.now()
    # fig = plt.figure()
    # ax = fig.add_subplot(1, 1, 1)
    # eod_data_manager.generate_stock_closed_price_plot(start_date,end_date,"FB",ax)
    # plt.show()

    #eod_data_manager.daily_run()
    #eod_data_manager.daily_bar_data_upload()
    #target_date =datetime.datetime.strptime("20150626", '%Y%m%d')
    #date_window= [datetime.datetime.strptime("20150624", '%Y%m%d'),datetime.datetime.strptime("20150625", '%Y%m%d')]
    #print eod_data_manager.filter_stock_by_volume(target_date,date_window,3.0)



    # for name in os.listdir("C:/dev/temp/eod"):
    #     if os.path.isdir(os.path.join("C:/dev/temp/eod", name)):
    #         des_folder = "C:/dev/temp"
    #         des_file = "OPRA_"+name+".csv"
    #         print "uploading "+des_file
    #         eod_data_manager.upload_option_csv_to_db(des_file,des_folder)

    #folder = "C:/dev/temp"
    #file_name = "OPRA_20150602.csv"
    #eod_data_manager.upload_option_csv_to_db(file_name,folder)




    print("--- %s seconds ---" % (time.time() - start_time))
