__author__ = 'Qing'

import logging
import BeautifulSoup
import urllib
import datetime
import re
import os

logger = logging.getLogger(__name__)

class YahooKeyStatDataCollector:
    def __init__(self, stock_list):
        self.stock_list = stock_list
        self.headers =["Shares Outstanding", "Float:", "% Held by Insiders","% Held by Institutions", "Shares Short (as of",
                       "Short Ratio (as of", "Short % of Float (as of", "Shares Short (prior month)"]


    # function to the yahoo link for key statistics page
    @staticmethod
    def get_web_page_link(symbol):
        link = "http://finance.yahoo.com/q/ks?s=" + symbol + "+Key+Statistics"
        return link

    # function to find the key statistics for the symbol
    def web_crawler(self, symbol, output_file, running_time):
        print "loading yahoo statistics for "+symbol
        try:
            html_text = urllib.urlopen(self.get_web_page_link(symbol))
            soup = BeautifulSoup.BeautifulSoup(html_text)
            title = soup.find(text ='Share Statistics')
            table= title.parent.parent.parent.parent
            content = {}
            for row in table.findAll("tr"):
                cells=row.findAll("td")
                if len(cells)==2:
                    for head in self.headers:
                        if head in cells[0].text.strip():
                            content[head]=self.convert_to_number(cells[1].text.strip())
                            break
            for head in self.headers:
                if head in content:
                    output_file.write(str(content[head]))
                    output_file.write(",")
                else:
                    output_file.write("-99,")
            output_file.write(symbol+","+running_time.strftime("%Y-%m-%d")+",")
            output_file.write("\n")
            output_file.flush()
        except:
            pass
        #for key, value in content.iteritems():
        #    print key, value

    def run(self, running_time, des_folder):
        path = des_folder+ "/daily_run/" + running_time.strftime('%Y_%m_%d')+"/yahoo/"
        if not os.path.exists(path):
            os.makedirs(path)

        transaction_date = running_time.strftime('%Y%m%d')
        des_file_name= path+"Key_Statistics_"+ transaction_date +".csv"
        with open(des_file_name, "w") as output_file:
            for head in self.headers:
                output_file.write(head+",")
            output_file.write("symbol,date,")
            output_file.write("\n")
            for symbol in self.stock_list:
                self.web_crawler(symbol, output_file, running_time)
                #time.sleep(2)

    def convert_to_number(self, number_string):
        if number_string=="N/A":
            return -99
        pattern_match = re.match("\d*\.?\d*[Mm]", number_string)
        if pattern_match:
            return int(float(number_string[:-1])*1000000)
        pattern_match=re.match("\d*\.?\d*[Bb]", number_string)
        if pattern_match:
            return int(float(number_string[:-1])*1000000000)
        pattern_match=re.match("\d*\.?\d*[Kk]", number_string)
        if pattern_match:
            return int(float(number_string[:-1])*1000)
        pattern_match=re.match("\d*\.?\d*%", number_string)
        if pattern_match:
            return float(number_string[:-1])*0.01
        pattern_match=re.match("\d*\.?\d*", number_string)
        if pattern_match:
            return float(number_string)

        return -99


