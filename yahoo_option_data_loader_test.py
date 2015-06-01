__author__ = 'qingluo'

import yahoo_option_data_loader
import time

if __name__ == "__main__":

    start_time = time.time()
    file=open("test.csv","w")
    data_loader=yahoo_option_data_loader.YahooOptionDataLoader()
    symbols=["FB","MMM","MCO","BF-B","O"]
    #symbols=["FB"]
    for symbol in symbols:
        data_loader.web_crawler(symbol,file)
    print("--- %s seconds ---" % (time.time() - start_time))




