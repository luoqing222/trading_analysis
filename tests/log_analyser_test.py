__author__ = 'Qing'

import sys,os
sys.path.append(os.path.realpath('..'))
from data_analyser import log_analyser

if __name__ == "__main__":
    file_location = "C:/dev/temp"
    file_name = "daily_run.log"
    keyword_list = ['WARNING']
    log_analyser=log_analyser.LogAnalyser(file_location, file_name,keyword_list)
    print log_analyser.findKeyWord()

