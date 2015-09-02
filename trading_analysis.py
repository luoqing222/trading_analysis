import MySQLdb

__author__ = 'qingluo'

import emailprocessing
import yahoo_data_analyser
import datetime
import models
import sys
from peewee import fn
import pandas as pd
import urllib
import BeautifulSoup
import re
import dictionary_ids
import trading_data_manager
import eoddata_data_manager
import yahoo_option_data_manager
import time
import os
import trading_date_utility
import logging
from data_collectors import eod_1minbar_data_collector
import configparser



def send_email(file_name, mail_list, folder):
    gm = emailprocessing.Gmail('raki1978wmc6731@gmail.com', 'Fapkc1897Fapkc')

    #subject = "analysis report on " + datetime.datetime.now().strftime('%m_%d_%Y')
    subject = file_name
    message = "This is a test message"
    # to = "luoqing222@gmail.com"

    # gm.send_message(subject, message, 'luoqing222@gmail.com')
    for to in mail_list:
        gm.send_text_attachment(subject, to, file_name, folder)

#
# def send_email2():
#     # Open a plain text file for reading.  For this example, assume that
#     # the text file contains only ASCII characters.
#     msg = MIMEMultipart()
#     text_file = "requirements.txt"
#     fp = open(text_file, 'rb')
#     # Create a text/plain message
#     attachment = MIMEText(fp.read())
#     fp.close()
#     me = "raki1978wmc6731@gmail.com"
#     you = "luoqing222@gmail.com"
#     msg['Subject'] = 'The contents of %s' % text_file
#     msg['From'] = me
#     msg['To'] = you
#     attachment.add_header("Content-Disposition", "attachment", filename=text_file)
#     msg.attach(attachment)
#
#     # Send the message via our own SMTP server, but don't include the
#     # envelope header.
#     server = smtplib.SMTP("smtp.gmail.com", 587)
#     server.ehlo()
#     server.starttls()
#     server.login(me, 'Fapkc1897Fapkc')
#     server.sendmail(me, [you], msg.as_string())
#     server.quit()

def initialize_holiday_table():
    models.db.connect()
    if not models.HolidayCalendar.table_exists():
        models.db.create_table(models.HolidayCalendar)
        trading_date_utility.add_holiday(2015, 5, 25, 'US')
        trading_date_utility.add_holiday(2015, 7, 3, 'US')
        trading_date_utility.add_holiday(2015, 9, 7, 'US')
        trading_date_utility.add_holiday(2015, 11, 26, 'US')
        trading_date_utility.add_holiday(2015, 11, 27, 'US')
        trading_date_utility.add_holiday(2015, 12, 25, 'US')


def initialize_index_fund_table():
    models.db.connect()
    if not models.IndexSymbol.table_exists():
        models.db.create_table(models.IndexSymbol)
        models.IndexSymbol.create(symbol='ivv', name='iShares Core S&P 500')
        models.IndexSymbol.create(symbol='spy', name='SPDR S&P 500 ETF')
        models.IndexSymbol.create(symbol='voo', name='Vanguard S&P 500 ETF')


def initialize_historical_price_table():
    models.db.connect()
    if not models.HistoricalPrice.table_exists():
        models.db.create_table(models.HistoricalPrice)


def update_sp500list_table(data_manager, current_date):
    de_list = data_manager.updateSp500(current_date)
    if de_list is not None:
        for s in de_list:
            print s, "has been removed from S&P"
    return


logger = logging.getLogger(__name__)

if __name__ == "__main__":
    data_manager = trading_data_manager.TradingDataManager()
    current_date = datetime.datetime.now().date()
    update_sp500list_table(data_manager, current_date)

    # check if the day that is not trading day, stop running
    if not trading_date_utility.is_trading_day(datetime.datetime.now(), "US"):
        sys.exit(0)

    initialize_holiday_table()
    initialize_index_fund_table()
    initialize_historical_price_table()

    # make the directory for the messages to monitor
    current_folder = os.getcwd()
    message_folder = current_folder + "/" + "messages"
    if not os.path.exists(message_folder):
        os.makedirs(message_folder)

    running_time = datetime.datetime.now()
    log_file_name = "daily_run.log"
    log_file = message_folder+"/"+log_file_name
    logging.basicConfig(filename=log_file, level=logging.INFO,filemode="w")
    logger.info("start data collection process on "+running_time.strftime('%m_%d_%Y'))
    mail_list = ["luoqing222@gmail.com"]

    config_file = "option_data_management_setting.ini"
    config = configparser.ConfigParser()
    config.read(config_file)

    #to download eod data
    try:
        logger.info("Start collecting eod data")
        eod_data_manager = eoddata_data_manager.EodDataDataManager()
        eod_data_manager.daily_run()
        logger.info("eod data is successfully downloaded")
    except Exception, e:
        logger.warning("exception is thrown when downloading eod data:"+str(e))

    #to download yahoo option data
    try:
        logger.info("Start collecting yahoo option data")
        option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()
        option_data_manager.daily_run()
        logger.info("yahoo option data is successfully downloaded")
    except Exception, e:
        logger.warning("exception is thrown when downloading yahoo option data:"+str(e))

    #to download 1 bar min data
    try:
        logger.info("Start collecting 1 minute bar data from eod")
        driver_location = config.get("driver", "chrome_driver")
        download_folder = config.get("driver","download_folder")
        username = config.get("eod", "user")
        password = config.get("eod", "passwd")
        des_folder = config.get("csv","data_folder")
        data_collector = eod_1minbar_data_collector.Eod1MinBarDataCollector(driver_location,username,password)
        data_collector.run(download_folder, des_folder, running_time)
        logger.info("eod 1 min bar data is successfully downloaded")
    except Exception, e:
        logger.warning("exception is thrown when downloading eod 1 minute bar data: "+str(e))

    send_email(log_file_name, mail_list, message_folder)


    #function to
    #print("--- %s seconds ---" % (time.time() - start_time))
