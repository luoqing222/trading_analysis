__author__ = 'qingluo'

import configparser
import datetime
import re
import os
from ftplib import FTP
import shutil
import models
import MySQLdb
from _mysql_exceptions import IntegrityError
import pandas.io.sql as sql
import pandas as pd


class EodDataDataManager:
    def __init__(self):
        self.config_file = "option_data_management_setting.ini"
        pass

    def decompose_option_contract(self, contract_name):
        result = []
        option_name = re.compile("^[A-Z]+\.*[A-Z]+\d{6}[CP]\d{8}")
        if not option_name.match(contract_name):
            return None
        underlying_stock = re.sub("\d{6}[CP]\d{8}", "", contract_name)
        contract_name = re.sub("^[A-Z]+\.*[A-Z]+", "", contract_name)
        expiration_date = re.sub("[CP]\d{8}", "", contract_name)
        contract_name = re.sub("^\d{6}", "", contract_name)
        option_type = re.sub("\d{8}", "", contract_name)
        contract_name = re.sub("^[CP]", "", contract_name)
        strike_price = contract_name

        # print "underlying_stock is ", underlying_stock
        # print "expiration_date is ", expiration_date
        # print "option_type is ", option_type
        # print "strike_price is ", strike_price
        return [underlying_stock, expiration_date, option_type, strike_price]

    # this function convert the eod option data .txt format into .csv format
    def option_txt_to_csv(self, src_folder, src_file, des_folder, des_file):
        src_file_name = src_folder + "/" + src_file
        des_file_name = des_folder + "/" + des_file

        # if the file doesn't exist
        if not os.path.exists(src_file_name):
            print src_file_name + " does not exists at all!"
            return

        # print des_file_name
        output = open(des_file_name, "w")
        with open(src_file_name) as fp:
            for line in fp:
                [contract_name, date, open_price, high, low, close, volume, open_interest] = line.split(",")
                temp = self.decompose_option_contract(contract_name)
                if temp is not None:
                    underlying_stock = temp[0]
                    expiration_date = temp[1]
                    option_type = temp[2]
                    strike_price = str(int(temp[3]))
                    date = datetime.datetime.strptime(date, "%Y%m%d").strftime("%Y/%m/%d")
                    expiration_date = datetime.datetime.strptime(expiration_date, "%y%m%d").strftime("%Y/%m/%d")
                    output.write(date + "," + underlying_stock + "," + option_type + "," + expiration_date + ",")
                    output.write(
                        strike_price + "," + contract_name + "," + open_price + "," + high + "," + low + "," + close + "," + volume + "," + open_interest)
                    # output.write(underlying_stock + "," + expiration_date + "," + option_type+"," + strike_price+",")
                    # output.write(date+ "," + open_price + "," + high + "," + low + "," + close + "," + volume + "," + open_interest)

    # this function to copy the equity data .txt format into .csv format
    def copy_txt_to_csv(self, src_folder, src_file, des_folder, des_file):
        src_file_name = src_folder + "/" + src_file
        des_file_name = des_folder + "/" + des_file
        # if the file doesn't exist
        if not os.path.exists(src_file_name):
            print src_file_name + " does not exists at all!"
            return

        shutil.copyfile(src_file_name, des_file_name)

    def get_eod_data_dir(self, config, running_time):
        '''
        :param config: config file
        :param running_time: running time
        :return: the location where yahoo data should be located
        '''
        des_folder = config.get("csv", "data_folder") + "/" + "daily_run" + "/" + running_time.strftime(
            "%Y_%m_%d") + "/" + "eod"
        if not os.path.exists(des_folder):
            os.makedirs(des_folder)
        return des_folder

    def upload_equity_csv_to_db(self, config, file_name, folder):
        ''' this function is to upload csv files into database
        :param config: config
        :param file_name: file name
        :param folder: folder
        :return:
        '''
        models.db.connect()
        if not models.EodEquity.table_exists():
            models.db.create_table(models.EodEquity)
        des_file_name = folder + "/" + file_name
        exchange = file_name.split("_")[0]
        records = []
        if os.path.exists(des_file_name):
            with open(des_file_name) as fp:
                for line in fp:
                    splited_item = line.split(',')
                    if len(splited_item) == 7:
                        [symbol, transaction_date, open_price, high_price, low_price, close_price,
                         volume] = splited_item
                        transaction_date = datetime.datetime.strptime(transaction_date, '%Y%m%d').strftime("%Y-%m-%d")
                        try:
                            records.append((symbol, transaction_date, float(open_price), float(high_price),
                                            float(low_price), float(close_price), int(volume.strip()), exchange))
                        except:
                            pass

        host = config.get("database", "host")
        database = config.get("database", "database")
        user = config.get("database", "user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)
        cursor = db.cursor()
        sql_statement = "insert into eodequity(symbol,transaction_date,open_price, high_price,low_price, close_price,volume, exchange) values(%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cursor.executemany(sql_statement, records)
            db.commit()
        except:
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()

    def upload_option_csv_to_db(self, config, file_name, folder):
        #config = configparser.ConfigParser()
        #config.read(self.config_file)
        models.db.connect()
        if not models.EodOption.table_exists():
            models.db.create_table(models.EodOption)
        des_file_name = folder + "/" + file_name
        record_map = {}
        if os.path.exists(des_file_name):
            with open(des_file_name) as fp:
                for line in fp:
                    splited_item = line.split(',')
                    if len(splited_item) == 12:
                        [transaction_date, underlying_stock, option_type, expire_date, strike_price,
                         contract, open_price, high_price, low_price, close_price, volume, open_interest] = splited_item
                        try:
                            record_map[
                                (transaction_date, underlying_stock, option_type, expire_date, int(strike_price))] = (
                                transaction_date, underlying_stock, option_type, expire_date, int(strike_price),
                                contract, float(open_price), float(high_price), float(low_price), float(close_price),
                                int(volume), int(open_interest.strip()))
                        except:
                            pass

        records = record_map.values()
        # print records
        host = config.get("database", "host")
        database = config.get("database", "database")
        user = config.get("database", "user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)

        cursor = db.cursor()

        sql_statement = "insert into eodoption(transaction_date, underlying_stock, option_type,expire_date,strike_price,contract, open_price, high_price, low_price,close_price, volume,open_interest) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cursor.executemany(sql_statement, records)
            db.commit()
        except IntegrityError:
            pass
        except:
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()

    def upload_bar_1min_equity_to_db(self, config, file_name, folder):
        ''' function to save 1 minute frequency trading data into database
        :param config: config object
        :param file_name: file name need to start with NASDAQ or NYSE
        :param folder:
        :return: None
        '''
        models.db.connect()
        if not models.Bar1MinEodData.table_exists():
            models.db.create_table(models.Bar1MinEodData)
        des_file_name = folder + "/" + file_name
        exchange = file_name.split("_")[0]
        records = []
        if os.path.exists(des_file_name):
            with open(des_file_name) as fp:
                next(fp)
                for line in fp:
                    splited_item = line.split(',')
                    if len(splited_item) == 7:
                        [symbol, transaction_time, open_price, high_price, low_price, close_price,
                         volume] = splited_item
                        transaction_time = datetime.datetime.strptime(transaction_time,'%d-%b-%Y %H:%M')
                        transaction_date = transaction_time.strftime("%Y-%m-%d")
                        transaction_time = transaction_time.strftime("%Y-%m-%d %H:%M")
                        try:
                            records.append((symbol, transaction_date, transaction_time,float(open_price), float(high_price),
                                            float(low_price), float(close_price), int(volume.strip()), exchange))
                        except:
                            pass
        #print records

        host = config.get("database", "host")
        database = config.get("database", "database")
        user = config.get("database", "user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)
        cursor = db.cursor()
        sql_statement = "insert into bar1mineoddata(symbol,transaction_date,transaction_time, open_price, high_price,low_price, close_price,volume, exchange) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cursor.executemany(sql_statement, records)
            db.commit()
        except:
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()

    def filter_stock_by_volume(self, target_date, date_window, filter_parameter):
        ''' function to list all the stocks that have relative huge volume compared with average volume in date_window
        :param target_date:
        :param date_window:
        :param filter_parameter:
        :return: the dictionary that contains the symbol and the symbol weighted price
        '''
        config = configparser.ConfigParser()
        config.read(self.config_file)

        host = config.get("database", "host")
        database = config.get("database","database")
        user = config.get("database","user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host,db=database, user=user, passwd=password)
        eod_equity_table_name = "eodequity"

        min_date = min(target_date,min(date_window))
        max_date = max(target_date,max(date_window))

        min_date_str = min_date.strftime('%Y-%m-%d')
        max_date_str = max_date.strftime('%Y-%m-%d')
        sql_statement = ("select * from %(table_name)s where transaction_date >= '%(begin_date)s' and transaction_date<='%(end_date)s'")
        eod_data= sql.read_sql(sql_statement%{'begin_date':min_date_str,'end_date':max_date_str, 'table_name': eod_equity_table_name},db)
        db.close()
        target_data = eod_data[eod_data.transaction_date == target_date.date()]
        target_data = target_data.set_index("symbol")

        date_window_in_date = [x.date() for x in date_window]
        bench_data = eod_data[eod_data['transaction_date'].apply(lambda x: x in date_window_in_date)]
        bench_avg_volume = bench_data.groupby("symbol")['volume'].mean()

        joined_data = target_data.join(bench_avg_volume,how='inner',rsuffix='_avg')
        filtered_data = joined_data[joined_data.volume/joined_data.volume_avg > filter_parameter]
        #print filtered_data
        return [x for x in filtered_data.index]

    def generate_stock_closed_price_plot(self, start_date, end_date, underlying_stock, ax):
        ''' function to draw the plot of stock closed price between start_date and end date, used in backward testing
        :param start_date:
        :param end_date:
        :param underlying_stock:
        :param ax:
        :return:
        '''
        config = configparser.ConfigParser()
        config.read(self.config_file)

        host = config.get("database", "host")
        database = config.get("database", "database")
        user = config.get("database", "user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)
        table_name = "eodequity"

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        sql_statement = (
            "select transaction_date, close_price from %(table_name)s where symbol"
            "='%(underlying_stock)s' and transaction_date between '%(begin_date)s' and '%(end_date)s' "
            "order by transaction_date asc")

        data_frame = sql.read_sql(
            sql_statement % {'begin_date': start_date_str, 'end_date': end_date_str, 'table_name': table_name, 'underlying_stock': underlying_stock}, db)
        stock_data = pd.Series(list(data_frame['close_price']),index = data_frame['transaction_date'])
        stock_data.plot(kind = 'line', ax = ax, title = underlying_stock )
        db.close()

    def daily_run(self):
        Config = configparser.ConfigParser()
        Config.read(self.config_file)

        # only download the data that is on running time day
        running_time_time = datetime.datetime.now()
        running_time = running_time_time.strftime("%Y%m%d")

        host = Config.get("eod", "host")
        user = Config.get("eod", "user")
        password = Config.get("eod", "passwd")
        des_folder = Config.get("csv", "temp_folder") + "/" + Config.get("eod", "data_folder")
        if not os.path.exists(des_folder):
            os.makedirs(des_folder)

        ftp = FTP(host=host, user=user, passwd=password)
        files = ftp.nlst()
        file_pattern = re.compile('[a-zA-Z]+_\d{8}.txt\Z')
        for f in files:
            if file_pattern.match(f):
                file_date = f.replace('.txt', "")
                file_date = re.sub('[a-zA-Z]+_', '', file_date)
                if file_date == running_time:
                    the_folder = des_folder + "/" + file_date
                    if not os.path.exists(the_folder):
                        os.makedirs(the_folder)
                    des_file_name = the_folder + "/" + f
                    print "downloading " + f + " to " + the_folder
                    ftp.retrbinary('RETR ' + f, open(des_file_name, 'wb').write)

        ftp.close()

        # process the txt files after it is downloaded
        src_folder = des_folder + "/" + running_time
        des_folder = self.get_eod_data_dir(Config, running_time_time)

        self.option_txt_to_csv(src_folder, "OPRA_" + running_time + ".txt", des_folder, "OPRA_" + running_time + ".csv")
        self.copy_txt_to_csv(src_folder, "NYSE_" + running_time + ".txt", des_folder, "NYSE_" + running_time + ".csv")
        self.copy_txt_to_csv(src_folder, "NASDAQ_" + running_time + ".txt", des_folder, "NASDAQ_" + running_time + ".csv")

        self.upload_equity_csv_to_db(Config, "NASDAQ_" + running_time + ".csv", des_folder)
        self.upload_equity_csv_to_db(Config, "NYSE_" + running_time + ".csv", des_folder)
        self.upload_option_csv_to_db(Config, "OPRA_" + running_time + ".csv", des_folder)

        txtfiles = [f for f in os.listdir(src_folder) if os.path.isfile(os.path.join(src_folder, f))]
        string_pattern = '[a-zA-Z]+_' + running_time + '.txt\Z'
        file_pattern = re.compile(string_pattern)
        for txtfile in txtfiles:
            if file_pattern.match(txtfile):
                self.copy_txt_to_csv(src_folder, txtfile, des_folder,txtfile)

    def daily_bar_data_upload(self):
        Config = configparser.ConfigParser()
        Config.read(self.config_file)
        file_name = "NASDAQ_BAR_1MIN_20150615.csv"
        folder = "C:/dev/data/eod/bar"
        self.upload_bar_1min_equity_to_db(Config, file_name, folder)





