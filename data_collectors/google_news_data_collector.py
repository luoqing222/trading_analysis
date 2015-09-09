__author__ = 'Qing'

import logging
import datetime
import urllib
import BeautifulSoup
import bs4
import re
import time
import os

logger = logging.getLogger(__name__)

class GoogleNewsDataCollector:
    def __init__(self, driver_location,nyse_list, nasdaq_list):
        self.driver_location = driver_location
        self.nyse_list = nyse_list
        self.nasdaq_list = nasdaq_list

    def download_nasdaq_news_including_day(self, running_time, nasdaq_symbol):
        ''' function to get the news statistics at NASDAQ during the day at running time, to run in the evening after the market is closed
        :param running_time:
        :param nasdaq_symbol:
        :return: the number of news. -99 means the exception is thrown.
        '''
        startdate = running_time.strftime("%Y-%m-%d")
        enddate = (running_time+datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        link = "https://www.google.com/finance/company_news?q=NASDAQ%3A"+nasdaq_symbol+"&startdate="+startdate+\
               "&enddate="+enddate
        try:
            html_text = urllib.urlopen(link)
            #print link
            soup = bs4.BeautifulSoup(html_text, "html.parser")
            mydivs = soup.findAll(text=re.compile('Showing stories'))
            news_number = mydivs[0].parent.text.strip().split(' ')[-1]
            if news_number == 'about':
                news_number = '0'
            return news_number
        except:
            return -99


    def download_nyse_news_including_day(self,running_time, nyse_symbol):
        ''' function to get the news statistics at NYSE during the day at running time, to run in the evening after the market is closed
        :param running_time:
        :param nyse_symbol:
        :return:
        '''
        startdate = running_time.strftime("%Y-%m-%d")
        enddate = (running_time+datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        link = "https://www.google.com/finance/company_news?q=NYSE%3A"+nyse_symbol+"&startdate="+startdate+\
               "&enddate="+enddate
        try:
            html_text = urllib.urlopen(link)
            #print link
            soup = bs4.BeautifulSoup(html_text, "html.parser")
            mydivs = soup.findAll(text=re.compile('Showing stories'))
            news_number = mydivs[0].parent.text.strip().split(' ')[-1]
            if news_number == 'about':
                news_number = '0'
            return news_number
        except:
            return -99

    def download_nasdaq_news_before_day(self, running_time, nasdaq_symbol):
        ''' function to get the news statistics of nasdaq before the day at running time, to run in the early morning
        :param running_time:
        :param nasdaq_symbol:
        :return:
        '''
        enddate = running_time.strftime("%Y-%m-%d")
        startdate = (running_time+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
        link = "https://www.google.com/finance/company_news?q=NASDAQ%3A"+nasdaq_symbol+"&startdate="+startdate+\
               "&enddate="+enddate
        try:
            html_text = urllib.urlopen(link)
            #print link
            soup = bs4.BeautifulSoup(html_text, "html.parser")
            mydivs = soup.findAll(text=re.compile('Showing stories'))
            news_number = mydivs[0].parent.text.strip().split(' ')[-1]
            if news_number == 'about':
                news_number = '0'
            return news_number
        except:
            return -99


    def download_nyse_news_before_day(self,running_time, nyse_symbol):
        ''' function to get the news statistics of nyse before the day at running time, to run in the early morning
        :param running_time:
        :param nyse_symbol:
        :return:
        '''
        startdate = (running_time+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
        enddate = running_time.strftime("%Y-%m-%d")
        link = "https://www.google.com/finance/company_news?q=NYSE%3A"+nyse_symbol+"&startdate="+startdate+\
               "&enddate="+enddate
        try:
            html_text = urllib.urlopen(link)
            #print link
            soup = bs4.BeautifulSoup(html_text, "html.parser")
            mydivs = soup.findAll(text=re.compile('Showing stories'))
            news_number = mydivs[0].parent.text.strip().split(' ')[-1]
            if news_number == 'about':
                news_number = '0'
            return news_number
        except:
            return '-99'

    def run(self, running_time, des_folder):
        path = des_folder+ "/daily_run/" + running_time.strftime('%Y_%m_%d')+"/google/"
        if not os.path.exists(path):
            os.makedirs(path)

        transaction_date = running_time.strftime('%Y%m%d')
        des_file_name= path+"News_Counts_"+ transaction_date +".csv"
        output_file = open(des_file_name, "w")
        for symbol in self.nasdaq_list:
            news_count = self.download_nasdaq_news_including_day(running_time, symbol)
            output_file.write(transaction_date+","+symbol+",NASDAQ,"+news_count+"\n")
            output_file.flush()
            time.sleep(2)
        for symbol in self.nyse_list:
            news_count = self.download_nyse_news_including_day(running_time, symbol)
            output_file.write(transaction_date+","+symbol+",NYSE,"+news_count+"\n")
            time.sleep(2)
