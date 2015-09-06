__author__ = 'Qing'

import datetime
import trading_date_utility
import urllib
import BeautifulSoup
import models
import re

class YahooEquityDataLoader:
    def __init__(self):
        pass

    @staticmethod
    def generate_download_link(start_date, end_date, symbol):
        '''
        :param start_date: starting date when downloading the .csv for equity
        :param end_date: end date when downloading the .csv for equity
        :param symbol: symbol for the equity
        :return:
        '''
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

    @staticmethod
    def generate_full_download_link(symbol):
        '''
        :param symbol: symbol for the equity
        :return: the web page to download the equity data
        '''
        link = "http://finance.yahoo.com/q/hp?s=" + symbol + "+Historical+Prices"
        return link

    def update_database(self, symbol, recent_date, country, full_download):
        if full_download:
            link = self.generate_full_download_link(symbol)
        else:
            #next_day = next_business_day(recent_date, country, 1)
            next_day = trading_date_utility.next_business_day(recent_date,country)
            link = self.generate_download_link(next_day, datetime.datetime.now(), symbol)

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

    def save_trading_data(self, symbol, max_date_in_table):
        if symbol in max_date_in_table:
            print "Updating database for symbol " + symbol
            self.update_database(symbol, max_date_in_table[symbol], "US", False)
    # if the symbol is not in the HistoricalPrice table, we need to get all the historical data
        else:
            print "Downloading Full history for symbol " + symbol
            self.update_database(symbol, None, "US", True)
