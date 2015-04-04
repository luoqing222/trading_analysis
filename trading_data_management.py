__author__ = 'qingluo'

import BeautifulSoup
import urllib
import models
import datetime

class TradingDataManager:
    def __init__(self):
        pass

    @staticmethod
    def generate_constituents(file_name):
        file = open(file_name, "w")
        link = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        html_text = urllib.urlopen(link)
        soup = BeautifulSoup.BeautifulSoup(html_text)
        table = soup.find('table', {'class': 'wikitable sortable'})
        th = table.findAll('th')
        for head in th:
            print head.text
        print len(th)
        td = table.findAll('td')
        for i in range(0,len(td)/len(th)):
            for j in range(0,len(th)):
                index = len(th)*i+j
                file.write(td[index].text+",")
            file.write("\n")
        file.close()

    @staticmethod
    def delete_holiday(year, month, day,country):
        models.db.connect()

        holiday = datetime.datetime(year, month, day)
        query= models.HolidayCalendar.delete().where(models.HolidayCalendar.date== holiday,
                                                     models.HolidayCalendar.country_code==country)
        query.execute()


    @staticmethod
    def add_holiday(year, month, day, country):
        models.db.connect()
        if not models.HolidayCalendar.table_exists():
            models.db.create_table(models.HolidayCalendar)

        holiday = datetime.datetime(year, month, day)
        models.HolidayCalendar.create(date=holiday, country_code= country)


    @staticmethod
    def populate_holiday():
        models.HolidayCalendar.drop_table()
        models.db.create_table(models.HolidayCalendar)
        TradingDataManager.add_holiday(2015, 4, 3, "US")
        TradingDataManager.add_holiday(2015, 5, 25, "US")
        TradingDataManager.add_holiday(2015, 7, 3, "US")
        TradingDataManager.add_holiday(2015, 9, 7, "US")
        TradingDataManager.add_holiday(2015, 11, 26, "US")
        TradingDataManager.add_holiday(2015, 11, 27, "US")
        TradingDataManager.add_holiday(2015, 12, 25, "US")





