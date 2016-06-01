__author__ = 'Qing'

from selenium import webdriver
from bs4 import BeautifulSoup
import re
import time
import logging
import urllib
import os.path
import shutil
from pyvirtualdisplay import Display

logger = logging.getLogger(__name__)


class SeekingAlphaIdeaDataCollector:
    def __init__(self, driver_location):
        self.driver_location = driver_location


        #self.driver = webdriver.Chrome()

        #self.driver = webdriver.Chrome(self.driver_location)
        self.loginPage="http://seekingalpha.com/account/login"
        self.username="luoqing222@gmail.com"
        self.password="Fapkc123"

    def download_ideas(self):
        '''
        function to crawling the  web page from www.seekingalpha.com and the comments
        :return:
        '''
        logger.info('Downloading Seeking Alpha Idea')

        #code below doesn't work on windows to use headless chrome driver
        #display = Display(size=(800, 800))
        #display.start()
        #driver = webdriver.Chrome(self.driver_location)

        self.driver.get("http://seekingalpha.com/stock-ideas/long-ideas")
        soup = BeautifulSoup(self.driver.page_source.encode('utf-8'),'html.parser')
        for div in soup.find_all('div', class_="media-body"):
            info_div= div.find('div',class_="a-info")
            if info_div:
                for link in div.find_all('a', class_="a-title"):
                    print link.text
                    print(link.get('href'))
                for span_area in info_div.find_all('span'):
                    if re.search("AM|PM",span_area.text):
                        for linkToSymbol in info_div.find_all('a',{ "href" : re.compile("symbol")}):
                            for author in info_div.find_all('a',{ "href" : re.compile("author")}):
                                print span_area.text
                                print(linkToSymbol.text)
                                print(author.text)
        self.driver.get("http://seekingalpha.com/article/3978343-promise-portola-pharmaceuticals")
        #print driver.page_source.encode('utf-8')

        #soup=BeautifulSoup(driver.page_source.encode('utf-8'),'html.parser')
        #code for the article

        #soup=BeautifulSoup(driver.page_source.encode('utf-8'),'html.parser')
        #code to get the summary of the article
        #summary_div=soup.find('div', class_="a-sum", itemprop="description")
        #code to get the content of the article
        #summary_div=soup.find('div', id="a-body", itemprop="articleBody")
        #if summary_div:
        #    if summary_div:
        #        for items in summary_div.find_all("p"):
        #            print items.get_text()

        #print driver.page_source.encode("utf-8")
        f = open('C:/dev/temp/article_source_code.html', 'w')
        f.write(self.driver.page_source.encode('utf-8'))
        f.close()


        #driver.close()
        #display.stop()
    def run_article_sentiment_analysis(self):
        self.driver.get("http://seekingalpha.com/article/3978343-promise-portola-pharmaceuticals")
        soup=BeautifulSoup(self.driver.page_source.encode('utf-8'),'html.parser')
        summary_div=soup.find('div', id="a-body", itemprop="articleBody")
        content=""
        if summary_div:
            for items in summary_div.find_all("p"):
                s=items.get_text().encode('utf-8')
                re.sub(r'[^\x00-\x7F]+',' ', s)
                content=content+s

            data = urllib.urlencode({"text": content})
            u = urllib.urlopen("http://text-processing.com/api/sentiment/", data)
            the_page = u.read()
            print the_page

    def run_comments_sentiment_analysis(self):
        pass

    def crawling_login(self):
        self.driver.get(self.loginPage)
        user_name = self.driver.find_element_by_id("login_user_email")
        user_name.send_keys(self.username)
        password = self.driver.find_element_by_id("login_user_password")
        password.send_keys(self.password)
        submit = self.driver.find_element_by_xpath("//input[@value='Sign in']")
        submit.click()
        time.sleep(30)

    def find_number_of_followers(self, stock_list, sleeping_time,driver):
        '''
        :param stock_list: list of the stock
        :param sleetping_time: num of second to sleep to let the web site update the number of followers
        :return: number for the followers for each stock.
        '''
        result = {}
        for stock in stock_list:
            try:
                print "find the number of follows for "+stock+"\n"
                driver.get("http://seekingalpha.com/symbol/"+stock)
                time.sleep(sleeping_time)
                soup=BeautifulSoup(driver.page_source.encode('utf-8'), 'html5lib')
                followers=soup.find('div', class_="followers-count")
                if followers:
                    text_string= followers.get_text().encode('utf-8')
                    num_of_followers= text_string.split(' ')[0]
                else:
                    num_of_followers="-99"
            except:
                num_of_followers="-99"

            print "the number is "+ num_of_followers+"\n"
            result[stock]=num_of_followers

        return result

            #f = open('C:/dev/temp/stock_webpage_code.html', 'w')
            #f.write(self.driver.page_source.encode('utf-8'))
            #f.close()




    def exitWebsite(self):
        self.driver.close()

    def run(self,running_time, des_folder, stock_list):
        display = Display(visible=0, size=(800, 800))
        display.start()
        driver = webdriver.Chrome()

        path = des_folder+ "/daily_run/" + running_time.strftime('%Y_%m_%d')+"/seekingAlpha/"
        if not os.path.exists(path):
            os.makedirs(path)
        transaction_date = running_time.strftime('%Y%m%d')
        des_file_name= path+"Followers_Counts_"+ transaction_date +".csv"

        #self.crawling_login()

        output_file = open(des_file_name, "w")
        # for symbol in self.nasdaq_list:
        #     news_count = self.download_nasdaq_news_including_day(running_time, symbol)
        #     output_file.write(transaction_date+","+symbol+",NASDAQ,"+news_count+"\n")
        #     output_file.flush()
        #     time.sleep(2)
        # for symbol in self.nyse_list:
        #     news_count = self.download_nyse_news_including_day(running_time, symbol)
        #     output_file.write(transaction_date+","+symbol+",NYSE,"+news_count+"\n")
        #     time.sleep(2)
        #stock_list=["FB","WMT","COP", "XX"]
        num_of_followers=self.find_number_of_followers(stock_list, 30)
        for key in num_of_followers:
            output_file.write(transaction_date+","+key+","+num_of_followers[key].replace(",", "")+"\n")

        driver.close()
        display.stop()

        #self.download_ideas()
        #self.run_article_sentiment_analysis()
        #self.exitWebsite()
