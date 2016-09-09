__author__ = 'Qing'

from selenium import webdriver
import time
import logging
import os.path
import shutil
from pyvirtualdisplay import Display

logger = logging.getLogger(__name__)

class Eod1MinBarDataCollector:
    def __init__(self, driver_location, username, password):
        self.driver_location = driver_location
        self.username = username
        self.password = password
        self.file_lists = ['NASDAQ', 'NYSE']

    def download_bar_data(self):
        '''
        function to download 1 min bar data from www.eoddata.com
        :return:
        '''
        logger.info('Downloading EOD 1 minute bar data')
        print self.driver_location
        #code below doesn't work on windows to use headless chrome driver
        #display = Display(visible=0, size=(800, 800))
        #display.start()
        driver = webdriver.Chrome(executable_path=self.driver_location)
        driver.get("http://www.eoddata.com/products/default.aspx")
        time.sleep(30)

        user_name = driver.find_element_by_id("ctl00_cph1_ls1_txtEmail")
        user_name.send_keys(self.username)
        password = driver.find_element_by_id("ctl00_cph1_ls1_txtPassword")
        password.send_keys(self.password)

        submit = driver.find_element_by_id("ctl00_cph1_ls1_btnLogin")
        submit.click()

        time.sleep(60)

        print driver.current_url
        el = driver.find_element_by_id('ctl00_cph1_cboQuickLinksPeriod')
        for option in el.find_elements_by_tag_name('option'):
            if option.text == '1 Minute Bars':
                option.click() # select() in earlier versions of webdriver
                break
        time.sleep(60)
        buttons = driver.find_elements_by_xpath("//input[@type='button'][@value='Download']")
        logger.info("downloaing Nasdaq 1 minute bar data")
        buttons[0].click()
        time.sleep(120)
        logger.info("downloading NYSE 1 minute bar data")
        buttons[1].click()
        time.sleep(120)
        driver.quit()
        #display.stop()

    def copy_download_files(self, download_folder, des_folder, file_name, date_time):
        '''
        :param download_folder: default download folder when downloading the eod bar data
        :param des_folder: the destination folder to be copied into
        :param file_name: file name
        :param date_time: data time type to specify the time that the file is downloaded
        :return: boolean flag to tell if the download is successful
        '''
        src_file_name= download_folder + "/" + file_name + "_"+ date_time.strftime('%Y%m%d') +".csv"
        print src_file_name
        des_file_name= des_folder+ "/daily_run/" + date_time.strftime('%Y_%m_%d')+"/eod/"+file_name + "_BAR_1MIN_"+ date_time.strftime('%Y%m%d') +".csv"
        if os.path.exists(src_file_name) and os.path.getsize(src_file_name)>0:
            logger.info("%s is successfully downloaded", src_file_name)
            try:
                shutil.move(src_file_name,des_file_name)
                logger.info("%s is successfully copied", des_file_name)
                return True
            except IOError as why:
                logger.warning("there is issues in copying downloaed file %s", src_file_name)
                logger.warning(str(why))
                return False
        else:
            logger.warning("Failed to download %s", src_file_name)
            return False

    def run(self, download_folder, des_folder, date_time):
        self.download_bar_data()
        for file_name in self.file_lists:
            self.copy_download_files(download_folder, des_folder, file_name, date_time)

