__author__ = 'Qing'



class YahooOptionDataCollector:
    def __init__(self, driver):
        self.driver = driver

    # function to the yahoo link for option page
    @staticmethod
    def get_web_page_link(symbol):
        link = "http://finance.yahoo.com/quote/" + symbol + "/Options"
        return link

    def download_options(self, symbol):
        print "loading yahoo option data for "+symbol
        link=self.get_web_page_link(symbol)
        self.driver.get(link)
        print self.driver.page_source.encode('utf-8')




