__author__ = 'qingluo'

import yahoo_option_data_manager
import time

if __name__ == "__main__":

    start_time = time.time()

    option_data_manager=yahoo_option_data_manager.YahooOptionDataManager()
    #option_data_manager.daily_run()
    option_data_manager.save_historical_data()

    print("--- %s seconds ---" % (time.time() - start_time))
