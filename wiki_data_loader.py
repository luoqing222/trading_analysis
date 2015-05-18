import urllib
import BeautifulSoup

__author__ = 'qingluo'


class WikiDataLoader:
    def __init__(self):
        pass

    @staticmethod
    def get_latest_sp500():
        try:
            link = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            html_text = urllib.urlopen(link)
            soup = BeautifulSoup.BeautifulSoup(html_text)
            table = soup.find('table', {'class': 'wikitable sortable'})
            th = table.findAll('th')
            td = table.findAll('td')
            result = []
            for i in range(0, len(td) / len(th)):
                index = len(th) * i + 0
                symbol= td[index].text
                result.append(symbol.replace(".","-"))
            return result
        except:
            return None


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
        for i in range(0, len(td) / len(th)):
            for j in range(0, len(th)):
                index = len(th) * i + j
                file.write(td[index].text + ",")
            file.write("\n")
        file.close()
