__author__ = 'qingluo'

import BeautifulSoup
import urllib

class TradingDatazManager:
    def __init__(self):
        pass

    @staticmethod
    def generate_constituents(file):
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



        # td = th.findNext('td')
        # while td:
        #     print td.text
        #     td=td.findNext('td')


