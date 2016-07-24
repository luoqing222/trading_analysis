__author__ = 'Qing'

from selenium import webdriver
import configparser
import platform
import os, sys
sys.path.append(os.path.realpath('..'))
from data_collectors import yahoo_option_data_collector_new

def start_local_chrome_driver():
    if platform.system() == "Windows":
        print "Running Under Windows"
        config_file = os.path.dirname(os.getcwd())+"/"+"option_data_management_setting.ini"
        config = configparser.ConfigParser()
        config.read(config_file)
        driver_location = config.get("driver", "chrome_driver")
        return webdriver.Chrome(driver_location)

    if platform.system() == "Linux":
        print "Running Under Linux"
        return webdriver.Chrome()

if __name__ == "__main__":

    symbol = "ELRC"
    driver = start_local_chrome_driver()
    data_collector = yahoo_option_data_collector_new.YahooOptionDataCollector(driver)

    with open("test.csv","w") as f:
        data_collector.web_crawler(symbol,f)

    driver.quit()






    #code under windows

    # code under linux




