__author__ = 'Qing'

from bs4 import BeautifulSoup
import datetime
import re
import time

class YahooOptionDataCollector:
    def __init__(self, driver):
        self.driver = driver

    # function to the yahoo link for option page
    @staticmethod
    def get_web_page_link(symbol):
        link = "http://finance.yahoo.com/quote/" + symbol + "/options"
        return link

    # function to get the option data for one expire date
    # the header in the file is under, type, expire date, strike, contract name, last, bid, ask, change, %change,
    # volume, open interest, implied volatility
    #@staticmethod
    def find_all_options_for_one_expire_date(self, web_page, expire_date, symbol,output_file):
        try:
            self.driver.get(web_page)
            soup = BeautifulSoup(self.driver.page_source.encode('utf-8'),'html.parser')
            option_tables = soup.findAll('table')
            for option_table in option_tables:
                if "calls" in option_table["data-reactid"]:
                    for row in option_table.findAll("tr"):
                        cols=row.findAll("td")
                        if cols:
                            output_file.write(symbol+",Calls,"+expire_date+",")
                            for col in cols:
                                temp = col.string.strip()
                                if ',' in temp:
                                    temp = temp.replace(',','')
                                output_file.write(temp + ",")
                            output_file.write("\n")

                if "puts" in option_table["data-reactid"]:
                    for row in option_table.findAll("tr"):
                        cols=row.findAll("td")
                        if cols:
                            output_file.write(symbol+",Puts,"+expire_date+",")
                            for col in cols:
                                temp = col.string.strip()
                                if ',' in temp:
                                    temp = temp.replace(',','')
                                output_file.write(temp + ",")
                            output_file.write("\n")

        except:
            pass

    def web_crawler(self, symbol, file):
        print "loading yahoo option data for "+symbol
        link=self.get_web_page_link(symbol)
        self.driver.get(link)
        soup = BeautifulSoup(self.driver.page_source.encode('utf-8'),'html.parser')
        for select_menu in soup.findAll("select"):
            if len(select_menu['data-reactid']) != 0:
                for options in select_menu.findAll("option"):
                    link=self.get_web_page_link(symbol)+"?date="+options['value']
                    expire_date=options.string.strip()
                    expire_date = datetime.datetime.strptime(expire_date, "%B %d, %Y")
                    expire_date = expire_date.strftime("%Y/%m/%d")
                    self.find_all_options_for_one_expire_date(link, expire_date, symbol,file)

        #link = "http://finance.yahoo.com/quote/FB/options?date=1470355200"
        #self.driver.get(link)
        #time.sleep(60)

        #soup = BeautifulSoup(self.driver.page_source.encode('utf-8'),'html.parser')

        #f = open('C:/dev/temp/article_source_code.html', 'w')
        #f.write(self.driver.page_source.encode('utf-8'))
        #f.close()

        #f = open('C:/dev/temp/article_source_code.html', 'w')
        #f.write(self.driver.page_source.encode('utf-8'))
        #f.close()
        #print self.driver.page_source.encode('utf-8')




