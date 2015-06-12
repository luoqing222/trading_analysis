__author__ = 'qingluo'

import datetime
import configparser
import os
import MySQLdb
import time
import yahoo_option_data_loader

class YahooOptionDataManager:
    def __init__(self):
        self.data_loader = yahoo_option_data_loader.YahooOptionDataLoader()
        self.config_file = "option_data_management_setting.ini"
        pass

    def get_symbol_list(self,config):
        host = config.get("database", "localhost")
        database = config.get("database","database")
        user = config.get("database","user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host,db=database, user=user, passwd=password)

        cursor = db.cursor()

        #find the mapping between symbol and adjust_close price
        #sql_statement = "select DISTINCT symbol from sp500list"
        sql_statement = "select DISTINCT symbol from nyselist"
        cursor.execute(sql_statement)
        rows = cursor.fetchall()
        result=[row[0] for row in rows]

        sql_statement = "select DISTINCT symbol from nasdaqlist"
        cursor.execute(sql_statement)
        rows = cursor.fetchall()
        for row in rows:
            result.append(row[0])

        sql_statement = "select DISTINCT symbol from indexsymbol"
        cursor.execute(sql_statement)
        rows = cursor.fetchall()
        for row in rows:
            result.append(row[0])
        db.close()

        result=list(set(result))
        return result


        #return ["FB","MMM","MCO","BF-B","O"]

    def get_file_name(self,running_time):
        return "yahoo_option_"+running_time.strftime("%Y_%m_%d")+".csv"

    def generate_temp_option_data(self,Config,running_time):
        temp_folder = Config.get("csv", "temp_folder")
        # check if the folder exist or not, if not then make it
        if not os.path.exists(temp_folder):
            os.makedirs(temp_folder)

        file_name = self.get_file_name(running_time)
        temp_full_file_name = temp_folder+"/"+file_name
        temp_data_file = open(temp_full_file_name,"w")
        symbol_list = self.get_symbol_list(Config)
        for symbol in symbol_list:
            self.data_loader.web_crawler(symbol,temp_data_file)
            #time.sleep(10)
        temp_data_file.close()

    def add_date_column_to_temp_data_file(self,Config,running_time):
        src_file_name= Config.get("csv","temp_folder")+"/"+self.get_file_name(running_time)

        des_folder= Config.get("csv","option_data_folder")
        if not os.path.exists(des_folder):
            os.makedirs(des_folder)
        des_file_name= des_folder+"/"+self.get_file_name(running_time)

        des_file=open(des_file_name,"w")
        with open(src_file_name) as fp:
            for line in fp:
                des_file.write(running_time.strftime("%Y/%m/%d")+","+line)

        des_file.close()


    def daily_run(self):
        Config = configparser.ConfigParser()
        Config.read(self.config_file)
        running_time=datetime.datetime.now()
        self.generate_temp_option_data(Config,running_time)
        self.add_date_column_to_temp_data_file(Config,running_time)








