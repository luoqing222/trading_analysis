__author__ = 'Qing'

import logging
import BeautifulSoup
import urllib
import datetime


logger = logging.getLogger(__name__)

class YahooKeyStatDataCollector:
    def __init__(self, stock_list):
        self.stock_list = stock_list

    # function to the yahoo link for key statistics page
    @staticmethod
    def get_web_page_link(symbol):
        link = "http://finance.yahoo.com/q/ks?s=" + symbol + "+Key+Statistics"
        return link

    # function to find the key statistics for the symbol
    def web_crawler(self, symbol):
        print "loading yahoo statistics for "+symbol
        html_text = urllib.urlopen(self.get_web_page_link(symbol))
        soup = BeautifulSoup.BeautifulSoup(html_text)
        title = soup.find(text ='Share Statistics')
        table= title.parent.parent.parent.parent
        for row in table.findAll("tr"):
            cells=row.findAll("td")
            if len(cells)==2:
                for cell in cells:
                    print cell.text.strip()

    def run(self):
        for symbol in self.stock_list:
            self.web_crawler(symbol)

