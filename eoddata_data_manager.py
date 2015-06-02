__author__ = 'qingluo'

import configparser
import datetime
import re
import os
from ftplib import FTP

class EodDataDataManager:
    def __init__(self):
        self.config_file = "option_data_management_setting.ini"
        pass

    def daily_run(self):
        Config = configparser.ConfigParser()
        Config.read(self.config_file)

        #only download the data that is on running time day
        running_time=datetime.datetime.now().strftime("%Y%m%d")

        host = Config.get("eod","host")
        user = Config.get("eod","user")
        password = Config.get("eod","passwd")
        des_folder = Config.get("csv", "temp_folder")+"/"+Config.get("eod", "data_folder")
        if not os.path.exists(des_folder):
            os.makedirs(des_folder)

        ftp = FTP(host=host, user=user, passwd = password)
        files = ftp.nlst()
        file_pattern = re.compile('[a-zA-Z]+_\d{8}.txt\Z')
        for f in files:
            if file_pattern.match(f):
                file_date = f.replace('.txt', "")
                file_date = re.sub('[a-zA-Z]+_','',file_date)
                if file_date == running_time:
                    the_folder= des_folder+"/"+file_date
                    if not os.path.exists(the_folder):
                        os.makedirs(the_folder)
                    des_file_name=the_folder+"/"+f
                    print "downloading "+f+" to "+the_folder
                    ftp.retrbinary('RETR '+f, open(des_file_name, 'wb').write)

        ftp.close()
