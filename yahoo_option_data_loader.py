__author__ = 'qingluo'

import BeautifulSoup
import urllib
import re
import time


class YahooOptionDataLoader:
    def __init__(self):
        pass

    # function to the yahoo link for option page
    @staticmethod
    def get_web_page_link(symbol):
        link = "http://finance.yahoo.com/q/op?s=" + symbol + "+Options"
        return link

    @staticmethod
    def find_all_options_for_one_expire_date(web_page):
        html_text = urllib.urlopen(web_page)
        soup = BeautifulSoup.BeautifulSoup(html_text)
        for option_type in soup.findAll('caption'):
            option_table= option_type.parent
            for row in option_table.findAll("tr"):
                if row.has_key("data-row"):
                    cols = row.findAll("td")
                    cols = [ele.text.strip() for ele in cols]
                    print cols



            #print "Option Type " + option_type.string.strip()


    def web_crawler(self, symbol):
        html_text = urllib.urlopen(self.get_web_page_link(symbol))
        soup = BeautifulSoup.BeautifulSoup(html_text)
        for expire_date_link in soup.findAll("option"):
            if len(expire_date_link['data-selectbox-link']) != 0:
                expire_date = expire_date_link.string.strip()
                link = "http://finance.yahoo.com/" + expire_date_link['data-selectbox-link']
                self.find_all_options_for_one_expire_date(link)

                #print time.strptime(expire_date_link.string.strip(), "%B %d, %Y")







