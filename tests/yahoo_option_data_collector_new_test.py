__author__ = 'Qing'

from selenium import webdriver
import platform
import os, sys
sys.path.append(os.path.realpath('..'))
from data_collectors import yahoo_option_data_collector_new

if __name__ == "__main__":
    if platform.system() == "Windows":
        print "Windows"
    elif platform.system() == "Linux":
        print "Linux"
    else:
        print "others"

    #symbol="FB"

    #code under windows

    # code under linux
    # driver = webdriver.Chrome()
    #data_collector = yahoo_option_data_collector_new.YahooOptionDataCollector(driver)
    #data_collector.download_options(symbol)
    #driver.quit()




