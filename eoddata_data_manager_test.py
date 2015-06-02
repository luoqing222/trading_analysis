__author__ = 'qingluo'

import eoddata_data_manager
import time

if __name__ == "__main__":

    start_time = time.time()

    eod_data_manager=eoddata_data_manager.EodDataDataManager()
    eod_data_manager.daily_run()

    print("--- %s seconds ---" % (time.time() - start_time))
