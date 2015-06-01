__author__ = 'qingluo'

import BeautifulSoup
import urllib
import datetime

class YahooOptionDataLoader:
    def __init__(self):
        pass

    # function to the yahoo link for option page
    @staticmethod
    def get_web_page_link(symbol):
        link = "http://finance.yahoo.com/q/op?s=" + symbol + "+Options"
        return link

    # function to get the option data for one expire date
    # the header in the file is under, type, expire date, strike, contract name, last, bid, ask, change, %change,
    # volume, open interest, implied volatility
    @staticmethod
    def find_all_options_for_one_expire_date(web_page, expire_date, symbol,output_file):
        try:
            html_text = urllib.urlopen(web_page)
            soup = BeautifulSoup.BeautifulSoup(html_text)
            for option_type in soup.findAll('caption'):
                option_table = option_type.parent
                for row in option_table.findAll("tr"):
                    if row.has_key("data-row"):
                        output_file.write(symbol+",")
                        output_file.write(option_type.string.strip() + ",")
                        output_file.write(expire_date + ",")
                        cols = row.findAll("td")
                        for col in cols:
                            output_file.write(col.text.strip() + ",")
                        output_file.write("\n")
        except:
            pass

    # function to find the option data for the symbol list
    def web_crawler(self, symbol, file):
        print "loading yahoo option data for "+symbol
        html_text = urllib.urlopen(self.get_web_page_link(symbol))
        soup = BeautifulSoup.BeautifulSoup(html_text)
        for expire_date_link in soup.findAll("option"):
            if len(expire_date_link['data-selectbox-link']) != 0:
                expire_date = expire_date_link.string.strip()
                link = "http://finance.yahoo.com/" + expire_date_link['data-selectbox-link']
                expire_date = datetime.datetime.strptime(expire_date, "%B %d, %Y")
                expire_date = expire_date.strftime("%Y/%m/%d")
                self.find_all_options_for_one_expire_date(link, expire_date, symbol,file)







