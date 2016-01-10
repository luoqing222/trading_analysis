__author__ = 'Qing'

import sys, os
import re

sys.path.append(os.path.realpath('..'))
from data_collectors import yahoo_keystatistics_data_collector


if __name__ == "__main__":
    stock_list = ['FB']
    data_collector = yahoo_keystatistics_data_collector.YahooKeyStatDataCollector(stock_list)
    data_collector.run()

