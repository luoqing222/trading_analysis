__author__ = 'qingluo'

import emailprocessing
import datetime
import models
import os
import sys
from peewee import fn
import pandas as pd
import urllib
import BeautifulSoup
import re
import dictionary_ids


def send_email():
    gm = emailprocessing.Gmail('raki1978wmc6731@gmail.com', 'Fapkc1897Fapkc')

    subject = "analysis report on " + datetime.datetime.now().strftime('%m_%d_%Y')
    message = "This is a test message"
    to = "luoqing222@gmail.com"
    text_file = "requirements.txt"

    # gm.send_message(subject, message, 'luoqing222@gmail.com')
    gm.send_text_attachment(subject, to, text_file)


def send_email2():
    # Open a plain text file for reading.  For this example, assume that
    # the text file contains only ASCII characters.
    msg = MIMEMultipart()
    text_file = "requirements.txt"
    fp = open(text_file, 'rb')
    # Create a text/plain message
    attachment = MIMEText(fp.read())
    fp.close()
    me = "raki1978wmc6731@gmail.com"
    you = "luoqing222@gmail.com"
    msg['Subject'] = 'The contents of %s' % text_file
    msg['From'] = me
    msg['To'] = you
    attachment.add_header("Content-Disposition", "attachment", filename=text_file)
    msg.attach(attachment)

    # Send the message via our own SMTP server, but don't include the
    # envelope header.
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.ehlo()
    server.starttls()
    server.login(me, 'Fapkc1897Fapkc')
    server.sendmail(me, [you], msg.as_string())
    server.quit()


def generate_download_link(start_date, end_date, symbol):
    dict = {0: '00', 1: '01', 2: '02', 3: '03', 4: '04', 5: '05', 6: '06', 7: '07', 8: '08', 9: '09', 10: '10',
            11: '11', 12: '12', 13: '13', 14: '14', 15: '15', 16: '16', 17: '17', 18: '18', 19: '19',
            20: '20', 21: '21', 22: '22', 23: '23', 24: '24', 25: '25', 26: '26', 27: '27', 28: '28', 29: '29',
            30: '30', 31: '31'}
    a = start_date.month - 1
    b = start_date.day
    c = start_date.year
    d = end_date.month - 1
    e = end_date.day
    f = end_date.year

    link = "http://finance.yahoo.com/q/hp?s=" + symbol + "&a=" + dict[a] + "&b=" + dict[b] + "&c=" + str(c) + "&d=" \
           + dict[d] + "&e=" + dict[e] + "&f=" + str(f) + "&g=d"

    return link


def generate_full_download_link(symbol):
    link = "http://finance.yahoo.com/q/hp?s=" + symbol + "+Historical+Prices"
    return link


def next_business_day(date, country, delta):
    next_day = pd.to_datetime(date) + pd.DateOffset(days=delta)
    (year, week, weekday) = next_day.isocalendar()
    year, month, day = next_day.year, next_day.month, next_day.day
    holiday = models.HolidayCalendar.select().where((models.HolidayCalendar.date.year == year)
                                                    & (models.HolidayCalendar.date.month == month)
                                                    & (models.HolidayCalendar.date.day == day)
                                                    & (models.HolidayCalendar.country_code == country))

    while (weekday > 5) or holiday.count() != 0:
        next_day = pd.to_datetime(next_day) + pd.DateOffset(days=delta)
        (year, week, weekday) = next_day.isocalendar()
        year, month, day = next_day.year, next_day.month, next_day.day
        holiday = models.HolidayCalendar.select().where((models.HolidayCalendar.date.year == year)
                                                        & (models.HolidayCalendar.date.month == month)
                                                        & (models.HolidayCalendar.date.day == day)
                                                        & (models.HolidayCalendar.country_code == country))

    return datetime.datetime(year, month, day)


def update_database(symbol, recent_date, country, full_download):
    if full_download:
        link = generate_full_download_link(symbol)
    else:
        next_day = next_business_day(recent_date, country, 1)
        link = generate_download_link(next_day, datetime.datetime.now(), symbol)
    # try:
    html_text = urllib.urlopen(link)
    soup = BeautifulSoup.BeautifulSoup(html_text)
    for tag in soup.findAll('a', href=True):
        if "real-chart" in tag['href']:
            models.db.connect()
            f = urllib.urlopen(tag['href'])
            line_number = 0
            for line in f:
                if 0 == line_number:
                    line_number += 1
                else:
                    transaction_data = re.split(r',', line)
                    models.HistoricalPrice.create(symbol=symbol, transaction_date=transaction_data[0],
                                                  open=float(transaction_data[1]), high=float(transaction_data[2]),
                                                  close=float(transaction_data[4]),
                                                  adjust_close=float(transaction_data[6]),
                                                  volume=long(transaction_data[5]))
                    # except:
                    # print link


def save_trading_data(symbol, max_date_in_table):
    # if the symbol is already in the HistoricalPrice table, we need to update the table
    if symbol in max_date_in_table:
        print "Updating database for symbol " + symbol
        update_database(symbol, max_date_in_table[symbol], "US", False)

    # if the symbol is not in the HistoricalPrice table, we need to get all the historical data
    else:
        print "Downloading Full history for symbol " + symbol
        update_database(symbol, None, "US", True)


def is_trading_day(date, country):
    (year, week, weekday) = date.isocalendar()
    # first check the weekday, not trading day if Saturday or Sunday
    if weekday > 5:
        return False
    # then check if it is national holiday
    year = date.year
    month = date.month
    day = date.day

    holiday = models.HolidayCalendar.select().where((models.HolidayCalendar.date.year == year)
                                                    & (models.HolidayCalendar.date.month == month)
                                                    & (models.HolidayCalendar.date.day == day)
                                                    & (models.HolidayCalendar.country_code == country))

    if holiday.count() != 0:
        return False

    return True


def add_holiday(year, month, day, country):
    models.db.connect()
    if not models.HolidayCalendar.table_exists():
        models.db.create_table(models.HolidayCalendar)

    holiday = datetime.datetime(year, month, day)
    models.HolidayCalendar.create(date=holiday, country_code=country)


def initialize_holiday_table():
    models.db.connect()
    if not models.HolidayCalendar.table_exists():
        models.db.create_table(models.HolidayCalendar)
        add_holiday(2015, 5, 25, 'US')
        add_holiday(2015, 7, 3, 'US')
        add_holiday(2015, 9, 7, 'US')
        add_holiday(2015, 11, 26, 'US')
        add_holiday(2015, 11, 27, 'US')
        add_holiday(2015, 12, 25, 'US')


def initialize_index_fund_table():
    models.db.connect()
    if not models.IndexSymbol.table_exists():
        models.db.create_table(models.IndexSymbol)
        models.IndexSymbol.create(symbol='ivv', name='iShares Core S&P 500')
        models.IndexSymbol.create(symbol='spy', name='SPDR S&P 500 ETF')
        models.IndexSymbol.create(symbol='voo', name='Vanguard S&P 500 ETF')


def initialize_sp500_table():
    models.db.connect()
    if not models.Sp500Symbol.table_exists():
        models.db.create_table(models.Sp500Symbol)
        file_name = os.path.dirname(sys.argv[0]) + "/" + "constituents.csv"
        with open(file_name) as f:
            for line in f:
                symbol, name, sector = line.split(",")
                models.Sp500Symbol.create(symbol=symbol, name=name, sector=sector,
                                          save_date=datetime.datetime.date(datetime.datetime.today()))


def initialize_historical_price_table():
    models.db.connect()
    if not models.HistoricalPrice.table_exists():
        models.db.create_table(models.HistoricalPrice)


def update_sp500_table():
    initialize_sp500_table()


def get_daily_returns(symbol, dates):
    daily_return_table = {}
    models.db.connect()
    for date in dates:
        # find the symbol's adjusted price
        data_record = models.HistoricalPrice.select().where((models.HistoricalPrice.symbol == symbol)
                                                            & (models.HistoricalPrice.transaction_date == date))
        if data_record.count() != 0:
            prev_date = next_business_day(date, "US", -1)
            prev_record = models.HistoricalPrice.select().where((models.HistoricalPrice.symbol == symbol)
                                                                & (
                models.HistoricalPrice.transaction_date == prev_date))
            if prev_record.count() != 0:
                ticker = dictionary_ids.DailyTickerID(symbol, date)
                daily_return_table[ticker] = data_record[0].adjust_close / prev_record[0].adjust_close - 1.0

    return daily_return_table


def get_average_daily_return(symbol, start_date, end_date):
    result = {}
    dates = []
    next_day = pd.to_datetime(start_date)
    while next_day <= end_date:
        dates.append(next_day)
        next_day = pd.to_datetime(next_day) + pd.DateOffset(days=1)

    daily_return_table = get_daily_returns(symbol, dates)
    result[symbol] = sum(daily_return_table.values()) / len(daily_return_table)
    return result


if __name__ == "__main__":
    send_email()
    initialize_holiday_table()
    initialize_index_fund_table()
    initialize_sp500_table()
    initialize_historical_price_table()


    # find the tick that in the HistoricalPrice
    query = models.HistoricalPrice.select(models.HistoricalPrice.symbol,
                                          fn.Max(models.HistoricalPrice.transaction_date).
                                          alias('recent_date')).group_by(models.HistoricalPrice.symbol)

    symbol_most_recent_date = {}
    # mapping from symbol to most recent date is saved in dictionary symbol_most_recent_date
    # including the index fund price
    for item in query:
        symbol_most_recent_date[item.symbol] = item.recent_date

    # get the SP500 list with most recent save date
    query = models.Sp500Symbol.select(fn.Max(models.Sp500Symbol.save_date).alias('recent_date'))
    for item in query:
        SP500_recent_date = item.recent_date

    # item in the query is latest sp500 tick
    query = models.Sp500Symbol.select().where(models.Sp500Symbol.save_date == SP500_recent_date)
    for item in query:
        symbol = item.symbol
        save_trading_data(symbol, symbol_most_recent_date)

    #do the same thing for index fund
    query = models.IndexSymbol.select()
    for item in query:
        symbol = item.symbol
        save_trading_data(symbol, symbol_most_recent_date)


        # step 2: run the data analysis


        #
        #  step 3: send the email#
        #send_email2()



