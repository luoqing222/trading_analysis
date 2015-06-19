__author__ = 'qingluo'

import datetime
import configparser
import os
import MySQLdb
import time
import yahoo_option_data_loader
import models

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


    def get_file_name(self,running_time):
        '''  to generate the file name when call the yahoo option manager. has been tested.
        :param running_time: the running time that the funciton is called
        :return: the file name
        '''
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
            temp_data_file.flush()
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

    def upload_csv_to_db(self, config, file_name):
        models.db.connect()
        if not models.YahooOption.table_exists():
            models.db.create_table(models.YahooOption)
        des_folder= config.get("csv", "option_data_folder")
        des_file_name = des_folder+ "/" + file_name
        #print des_file_name
        records = []
        if os.path.exists(des_file_name):
            with open(des_file_name) as fp:
                for line in fp:
                    line = line.replace('-','0')
                    splited_item = line.split(',')
                    if len(splited_item) == 15:
                        [transaction_date, underlying_stock, option_type, expire_date,strike_price,
                         contract, last, bid, ask,  price_change, pct_change, volume, open_interest, implied_vol,temp] = splited_item
                        try:
                            records.append((transaction_date, underlying_stock, option_type[0], expire_date,int(float(strike_price)*1000),
                             contract, float(last), float(bid), float(ask),  float(price_change), float(pct_change.strip('%'))/100.0,
                                            int(volume), int(open_interest), float(implied_vol.strip('%'))/100.0))
                        except:
                            pass

        host = config.get("database", "host")
        database = config.get("database","database")
        user = config.get("database","user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host,db=database, user=user, passwd=password)

        cursor = db.cursor()
        sql_statement = "insert into yahoooption(transaction_date, underlying_stock, option_type,expire_date,strike_price,contract, last, bid, ask,price_change, pct_change, volume,open_interest, implied_vol) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cursor.executemany(sql_statement, records)
            db.commit()
        except:
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()

    def daily_run(self):
        Config = configparser.ConfigParser()
        Config.read(self.config_file)
        running_time=datetime.datetime.now()
        self.generate_temp_option_data(Config,running_time)
        self.add_date_column_to_temp_data_file(Config,running_time)
        file_name= self.get_file_name(running_time)
        self.upload_csv_to_db(Config, file_name)








