__author__ = 'qingluo'

import datetime
import configparser
import os
import MySQLdb
import time
import yahoo_option_data_loader
import models
import pandas.io.sql as sql
import pandas as pd

class YahooOptionDataManager:
    def __init__(self):
        self.data_loader = yahoo_option_data_loader.YahooOptionDataLoader()
        self.config_file = "option_data_management_setting.ini"
        pass

    def get_symbol_list(self,config):
        host = config.get("database", "host")
        database = config.get("database","database")
        user = config.get("database","user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host,db=database, user=user, passwd=password)

        cursor = db.cursor()

        #find the mapping between symbol and adjust_close price
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
        #symbol_list =["FB"]
        for symbol in symbol_list:
            self.data_loader.web_crawler(symbol,temp_data_file)
            temp_data_file.flush()
        temp_data_file.close()

    def add_date_column_to_temp_data_file(self,Config,running_time):
        src_file_name= Config.get("csv","temp_folder")+"/"+self.get_file_name(running_time)

        des_folder = self.get_yahoo_data_dir(Config, running_time)
        des_file_name = des_folder + "/" + self.get_file_name(running_time)

        des_file=open(des_file_name,"w")
        with open(src_file_name) as fp:
            for line in fp:
                des_file.write(running_time.strftime("%Y/%m/%d")+","+line)

        des_file.close()

    def get_yahoo_data_dir(self, config, running_time):
        '''
        :param config: config file
        :param running_time: running time
        :return: the location where yahoo data should be located
        '''
        des_folder = config.get("csv", "data_folder") + "/" + "daily_run" + "/" + running_time.strftime("%Y_%m_%d") + "/" +"yahoo"
        if not os.path.exists(des_folder):
            os.makedirs(des_folder)
        return des_folder

    def upload_csv_to_db(self, config, file_name):
        '''
        :param config: config information to contain database information
        :param file_name: the file to be uploaded. file name includes the path
        :return:
        '''
        models.db.connect()
        if not models.YahooOption.table_exists():
            models.db.create_table(models.YahooOption)
        des_file_name = file_name
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

    def filter_stock_by_option_volume(self, analysis_date, date_window, filter_parameter):
        '''
        :param analysis_date: the date that is doing analysis
        :param date_window: the date window as the bench mark
        :param filter_parameter: the parameters
        :return: stock list
        '''
        config = configparser.ConfigParser()
        config.read(self.config_file)

        host = config.get("database", "host")
        database = config.get("database","database")
        user = config.get("database","user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host,db=database, user=user, passwd=password)
        table_name = "yahoooption"

        min_date = min(analysis_date,min(date_window))
        max_date = max(analysis_date,max(date_window))

        min_date_str = min_date.strftime('%Y-%m-%d')
        max_date_str = max_date.strftime('%Y-%m-%d')
        sql_statement = ("select * from %(table_name)s where transaction_date >= '%(begin_date)s' and transaction_date<='%(end_date)s'")
        data_frame= sql.read_sql(sql_statement%{'begin_date':min_date_str,'end_date':max_date_str, 'table_name': table_name},db)
        db.close()
        analysis_data = data_frame[data_frame.transaction_date == analysis_date.date()]
        analysis_data = analysis_data.groupby(["underlying_stock","option_type"])['volume'].sum()
        analysis_data.name = "sum"

        date_window_in_date = [x.date() for x in date_window]
        bench_data = data_frame[data_frame['transaction_date'].apply(lambda x: x in date_window_in_date)]
        bench_avg_volume = bench_data.groupby(["underlying_stock","option_type"])['volume'].sum()
        bench_avg_volume.name = "total"

        joined_data= pd.concat([analysis_data, bench_avg_volume], axis=1).reset_index()
        joined_data["ratio"]=joined_data["sum"]/joined_data["total"]
        #return joined_data
        filtered_data = joined_data[joined_data.ratio > filter_parameter]
        return [x for x in filtered_data.underlying_stock]


    def daily_run(self):
        Config = configparser.ConfigParser()
        Config.read(self.config_file)
        running_time=datetime.datetime.now()
        self.generate_temp_option_data(Config,running_time)
        self.add_date_column_to_temp_data_file(Config,running_time)
        file_name= self.get_yahoo_data_dir(Config,running_time)+"/"+self.get_file_name(running_time)
        self.upload_csv_to_db(Config, file_name)












