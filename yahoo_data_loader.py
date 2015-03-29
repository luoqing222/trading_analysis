__author__ = 'qingluo'

import BeautifulSoup
import urllib
import re
import time

class YahooOptionDataLoader:
    def __init__(self):
        pass

    #function to the link for option page
    @staticmethod
    def get_web_page_link(symbol):
        link = "http://finance.yahoo.com/q/op?s="+symbol+"+Options"
        print link
        return link

    def web_crawler(self, symbol):
        html_text = urllib.urlopen(self.get_web_page_link(symbol))
        soup = BeautifulSoup.BeautifulSoup(html_text)
        for expire_date_link in soup.findAll("option"):
            if len(expire_date_link['data-selectbox-link'])!= 0:
                print expire_date_link['data-selectbox-link']
                print expire_date_link.string.strip()
                print time.strptime(expire_date_link.string.strip(), "%B %d, %Y")


    def web_crawler_one_page(self, web_page):
        html_text= urllib.urlopen(web_page)
        soup = BeautifulSoup.BeautifulSoup(html_text)
        for option_type in soup.findAll('caption'):
            print "Option Type "+ option_type.string.strip()




