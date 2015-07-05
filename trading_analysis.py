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

    #function to collect the end of day NYSE, Dasdaq and option data
    #data is saved in the data_folder defined in option_data_management_setting.ini
    mail_list = ["luoqing222@gmail.com"]
    file_name = "eod_data_download_" + datetime.datetime.now().strftime('%m_%d_%Y') + ".csv"
    file_stream = open(message_folder + "/" + file_name, "w")
    file_stream.write("begin downloading eod data")
    try:
        eod_data_manager = eoddata_data_manager.EodDataDataManager()
        eod_data_manager.daily_run()
    except Exception, e:
        file_stream.write(str(e))
    file_stream.close()
    send_email(file_name,mail_list, message_folder)

    #function to download the yahoo option data
    mail_list = ["luoqing222@gmail.com"]
    file_name = "yahoo_option_data_download_" + datetime.datetime.now().strftime('%m_%d_%Y') + ".csv"
    file_stream = open(message_folder + "/" + file_name, "w")
    file_stream.write("begin download yahoo option data")
    start_time = time.time()
    try:
        option_data_manager = yahoo_option_data_manager.YahooOptionDataManager()
        option_data_manager.daily_run()
    except Exception, e:
        file_stream.write(str(e))
    file_stream.close()
    send_email(file_name, mail_list, message_folder)

    #collect_yahoo_sp500_data()
    print("--- %s seconds ---" % (time.time() - start_time))
