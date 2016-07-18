__author__ = 'qingluo'

import datetime
import configparser
import os
import MySQLdb
import time
#import yahoo_option_data_loader
import models
import pandas.io.sql as sql
import pandas as pd
import re
import data_collectors.yahoo_option_data_collector_new
import platform
from selenium import webdriver
from data_collectors import yahoo_option_data_collector_new

class YahooOptionDataManager:
    def __init__(self):
        #self.data_loader = yahoo_option_data_loader.YahooOptionDataLoader()
        self.config_file = "option_data_management_setting.ini"
        self.driver = self.start_local_chrome_driver()
        self.data_loader = yahoo_option_data_collector_new.YahooOptionDataCollector(self.driver)
        pass

    def __del__(self):
        self.driver.quit()

    def start_local_chrome_driver(self):
        if platform.system() == "Windows":
            print "Running Under Windows"
            config = configparser.ConfigParser()
            config.read(self.config_file)
            driver_location = config.get("driver", "chrome_driver")
            return webdriver.Chrome(driver_location)

        if platform.system() == "Linux":
            print "Running Under Linux"
            return webdriver.Chrome()


    def get_symbol_list(self, config):
        host = config.get("database", "host")
        database = config.get("database", "database")
        user = config.get("database", "user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)

        cursor = db.cursor()

        # find the mapping between symbol and adjust_close price
        sql_statement = "select DISTINCT symbol from nyselist"
        cursor.execute(sql_statement)
        rows = cursor.fetchall()
        result = [row[0] for row in rows]

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

        result = list(set(result))
        return result

    def get_file_name(self, running_time):
        '''  to generate the file name when call the yahoo option manager. has been tested.
        :param running_time: the running time that the funciton is called
        :return: the file name
        '''
        return "yahoo_option_" + running_time.strftime("%Y_%m_%d") + ".csv"

    def generate_temp_option_data(self, Config, running_time):
        temp_folder = Config.get("csv", "temp_folder")
        # check if the folder exist or not, if not then make it
        if not os.path.exists(temp_folder):
            os.makedirs(temp_folder)

        file_name = self.get_file_name(running_time)
        temp_full_file_name = temp_folder + "/" + file_name
        temp_data_file = open(temp_full_file_name, "w")
        symbol_list = self.get_symbol_list(Config)
        # symbol_list =["FB"]
        for symbol in symbol_list:
            self.data_loader.web_crawler(symbol, temp_data_file)
            temp_data_file.flush()
        temp_data_file.close()

    def add_date_column_to_temp_data_file(self, Config, running_time):
        src_file_name = Config.get("csv", "temp_folder") + "/" + self.get_file_name(running_time)

        des_folder = self.get_yahoo_data_dir(Config, running_time)
        des_file_name = des_folder + "/" + self.get_file_name(running_time)

        des_file = open(des_file_name, "w")
        with open(src_file_name) as fp:
            for line in fp:
                des_file.write(running_time.strftime("%Y/%m/%d") + "," + line)

        des_file.close()

    def get_yahoo_data_dir(self, config, running_time):
        '''
        :param config: config file
        :param running_time: running time
        :return: the location where yahoo data should be located
        '''
        des_folder = config.get("csv", "data_folder") + "/" + "daily_run" + "/" + running_time.strftime(
            "%Y_%m_%d") + "/" + "yahoo"
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
                    line = line.replace('-', '0')
                    splited_item = line.split(',')
                    if len(splited_item) == 15:
                        [transaction_date, underlying_stock, option_type, expire_date, strike_price,
                         contract, last, bid, ask, price_change, pct_change, volume, open_interest, implied_vol,
                         temp] = splited_item
                        try:
                            records.append((transaction_date, underlying_stock, option_type[0], expire_date,
                                            int(float(strike_price) * 1000),
                                            contract, float(last), float(bid), float(ask), float(price_change),
                                            float(pct_change.strip('%')) / 100.0,
                                            int(volume), int(open_interest), float(implied_vol.strip('%')) / 100.0))
                        except:
                            pass

        host = config.get("database", "host")
        database = config.get("database", "database")
        user = config.get("database", "user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)

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

    @staticmethod
    def decompose_option_contract(contract_name):
        result = []
        #option_name = re.compile("^[A-Z]+\.*[A-Z]+\d{6}[CP]\d{8}")
        option_name = re.compile("^[A-Z][A-Z]*\d{6}[CP]\d{8}")
        if not option_name.match(contract_name):
            return None
        underlying_stock = re.sub("\d{6}[CP]\d{8}", "", contract_name)
        contract_name = re.sub("^[A-Z][A-Z]*", "", contract_name)
        expiration_date = re.sub("[CP]\d{8}", "", contract_name)
        contract_name = re.sub("^\d{6}", "", contract_name)
        option_type = re.sub("\d{8}", "", contract_name)
        contract_name = re.sub("^[CP]", "", contract_name)
        strike_price = contract_name

        return [underlying_stock, expiration_date, option_type, strike_price]

    def filter_stock_by_option_volume(self, analysis_date, date_window, filter_parameter):
        ''' function to select the contract that has significant volume change
        :param analysis_date: the date that is doing analysis
        :param date_window: the date window as the bench mark
        :param filter_parameter: the parameters
        :return: stock list
        '''
        config = configparser.ConfigParser()
        config.read(self.config_file)

        host = config.get("database", "host")
        database = config.get("database", "database")
        user = config.get("database", "user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)
        table_name = "yahoooption"

        min_date = min(analysis_date, min(date_window))
        max_date = max(analysis_date, max(date_window))

        min_date_str = min_date.strftime('%Y-%m-%d')
        max_date_str = max_date.strftime('%Y-%m-%d')
        sql_statement = (
            "select * from %(table_name)s where transaction_date >= '%(begin_date)s' and transaction_date<='%(end_date)s'")
        data_frame = sql.read_sql(
            sql_statement % {'begin_date': min_date_str, 'end_date': max_date_str, 'table_name': table_name}, db)

        # get the equity data from eodequity
        sql_statement = (
            "select symbol as underlying_stock, volume as stock_volume, close_price from eodequity where transaction_date ='%(transaction_date)s'")
        eod_equity_frame = sql.read_sql(sql_statement % {'transaction_date': analysis_date.date()}, db)
        db.close()
        eod_equity_frame.to_csv("eod_equity.csv")

        analysis_all_data = data_frame[data_frame.transaction_date == analysis_date.date()]
        analysis_data = analysis_all_data.groupby(["underlying_stock", "option_type"])['volume'].sum()
        analysis_data.name = "sum"

        date_window_in_date = [x.date() for x in date_window]
        bench_data = data_frame[data_frame['transaction_date'].apply(lambda x: x in date_window_in_date)]
        bench_avg_volume = bench_data.groupby(["underlying_stock", "option_type"])['volume'].sum()
        bench_avg_volume.name = "total"

        joined_data = pd.concat([analysis_data, bench_avg_volume], axis=1).reset_index()
        joined_data = joined_data[joined_data["total"] != 0]
        joined_data["ratio"] = joined_data["sum"] / joined_data["total"]
        # return joined_data
        filtered_data = joined_data[joined_data.ratio > filter_parameter]
        stock_list = [x for x in filtered_data.underlying_stock]
        type_list = [y for y in filtered_data.option_type]

        # pick the selected stock
        stock_selected = []
        for underlying_stock, option_type in zip(stock_list, type_list):
            if stock_list.count(underlying_stock) == 1:
                stock_selected.append((underlying_stock, option_type))

        # calculate the statistics for each stock selected
        result_table = pd.DataFrame()
        for underlying_stock, option_type in stock_selected:
            #print underlying_stock, option_type
            option_data = analysis_all_data[(analysis_all_data.underlying_stock == underlying_stock) & (
                analysis_all_data.option_type == option_type)]
            total_option_volume = sum(option_data['volume'])
            option_data['total_option_volume'] = pd.Series([total_option_volume] * len(option_data),
                                                           index=option_data.index)
            #result_table = result_table.append(option_data.ix[option_data['volume'].idxmax()])
            result_table = result_table.append(option_data.loc[option_data['volume'].idxmax()])
        result_table = pd.merge(result_table, eod_equity_frame, how='left', on=['underlying_stock'])
        result_table['volume_ratio'] = 100 * result_table['volume'] / result_table['stock_volume']
        result_table['option_ratio'] = result_table['volume'] / result_table['total_option_volume']
        result_table['option_cost'] = 100 * result_table['volume'] * result_table['last']
        result_table['price_ratio'] = result_table['close_price'] / result_table['last']
        # result_table.to_csv("test.csv")
        # stock_list = [x for x in result_table.underlying_stock]
        # expire_date = [x for x in result_table.expire_date]
        # return [x for x in result_table.contract]
        return result_table

    def generate_option_sum_bar(self, start_date, end_date, underlying_stock, ax, expire_date):
        ''' function to generate the bar plot for underlying_stock
        :param start_date: start date
        :param end_date:
        :param underlying_stock:
        :param ax:
        :param expire_date: Only for illustration purpose, not essential in the calculation, can be any value
        :return:
        '''
        config = configparser.ConfigParser()
        config.read(self.config_file)

        host = config.get("database", "host")
        database = config.get("database", "database")
        user = config.get("database", "user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)
        table_name = "yahoooption"

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        sql_statement = (
            "select transaction_date, sum(volume) from %(table_name)s where underlying_stock "
            "= '%(underlying_stock)s' and transaction_date between '%(begin_date)s' and '%(end_date)s' "
            "and option_type = '%(option_type)s' group by transaction_date order by transaction_date asc")
        option_type = 'C'
        data_frame = sql.read_sql(
            sql_statement % {'begin_date': start_date_str, 'end_date': end_date_str, 'table_name': table_name,
                             'option_type': option_type, 'underlying_stock': underlying_stock}, db)
        bar_data_C = pd.Series(list(data_frame['sum(volume)']), index=data_frame['transaction_date'], name=option_type)

        option_type = 'P'
        data_frame = sql.read_sql(
            sql_statement % {'begin_date': start_date_str, 'end_date': end_date_str, 'table_name': table_name,
                             'option_type': option_type, 'underlying_stock': underlying_stock}, db)
        bar_data_P = pd.Series(list(data_frame['sum(volume)']), index=data_frame['transaction_date'], name=option_type)

        bar_data = pd.concat([bar_data_C, bar_data_P], axis=1)
        bar_data.plot(kind='bar', ax=ax, title=underlying_stock + "(" + expire_date + ")")
        db.close()

    def generate_contract_bar(self, start_date, end_date, contract, ax):
        ''' function to generate the bar with given contract between start date and end date
        :param start_date:
        :param end_date:
        :param contract:
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
        table_name = "yahoooption"

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        sql_statement = (
            "select transaction_date, sum(volume) from %(table_name)s where contract "
            "= '%(contract)s' and transaction_date between '%(begin_date)s' and '%(end_date)s' "
            " group by transaction_date order by transaction_date asc")
        data_frame = sql.read_sql(
            sql_statement % {'begin_date': start_date_str, 'end_date': end_date_str, 'table_name': table_name,
                             'contract': contract}, db)

        stock_data = pd.Series(list(data_frame['sum(volume)']), index=data_frame['transaction_date'], name='volume')
        stock_data.plot(kind='bar', ax=ax, title=contract)
        db.close()

    def save_abnormal_options(self, result_table):
        config = configparser.ConfigParser()
        config.read(self.config_file)

        models.db.connect()
        if not models.AbnormalOption.table_exists():
            models.db.create_table(models.AbnormalOption)
        if result_table is not None:
            records = []
            for index, row in result_table.iterrows():
                records.append((float(row['ask']), float(row['bid']), row['contract'], row['expire_date'],
                               float(row['implied_vol']), float(row['last']), \
                               int(row['open_interest']), row['option_type'], float(row['pct_change']),
                               float(row['price_change']), int(row['strike_price']), \
                               int(row['total_option_volume']), row['transaction_date'], row['underlying_stock'],
                               int(row['volume']), int(row['stock_volume']), \
                               float(row['close_price']), float(row['volume_ratio']), float(row['option_ratio']),
                               float(row['option_cost']), float(row['price_ratio'])))
            host = config.get("database", "host")
            database = config.get("database", "database")
            user = config.get("database", "user")
            password = config.get("database", "passwd")
            db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)

            cursor = db.cursor()
            sql_statement = "insert into abnormaloption(ask,bid,contract,expire_date,implied_vol,last,open_interest,\
            option_type,pct_change,price_change,strike_price,total_option_volume,transaction_date,underlying_stock,\
            contract_volume,stock_volume,stock_close_price,option_stock_volume_ratio,contract_total_option_volume_ratio,\
            option_cost,stock_price_option_price_ratio) \
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            try:
                cursor.executemany(sql_statement, records)
                db.commit()
            except:
                db.rollback()
                raise
            finally:
                cursor.close()
                db.close()

    def create_backward_testing_report(self,expire_date):
        config = configparser.ConfigParser()
        config.read(self.config_file)
        host = config.get("database", "host")
        database = config.get("database", "database")
        user = config.get("database", "user")
        password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)

        sql_statement = (
            "select transaction_date, underlying_stock, option_type, strike_price,contract, open_interest, "
            "total_option_volume,contract_volume, stock_volume, stock_close_price as stock_close_price_transaction_date, "
            "option_stock_volume_ratio,contract_total_option_volume_ratio, option_cost, stock_price_option_price_ratio,last "
            "from %(table_name)s where expire_date "
            "= '%(expire_date)s' ")

        table_name = 'abnormaloption'
        data_frame = sql.read_sql(
            sql_statement % {'table_name': table_name, 'expire_date': expire_date}, db)

        stocks= data_frame['underlying_stock']
        sql_str = "("
        for stock in stocks:
            sql_str+="'"+stock+"',"
        sql_str=sql_str[:-1]+")"

        table_name='eodequity'
        sql_statement = (
            "select symbol as underlying_stock, close_price as close_price_expire_date from %(table_name)s "
            "where transaction_date= '%(expire_date)s' and symbol in %(stock_list)s")
        stock_data_frame = sql.read_sql(
            sql_statement % {'table_name': table_name, 'expire_date': expire_date,'stock_list': sql_str}, db)

        result = pd.merge(data_frame, stock_data_frame, how='inner', on=['underlying_stock'])

        def calculate_final_gain_no_option_cost(option_type, close_price_transaction_date, close_price_expire_date):
            if option_type == 'P':
                if float(close_price_transaction_date) > close_price_expire_date:
                    return 1.0
                else:
                    return 0.0
            if option_type == 'C':
                if float(close_price_transaction_date) < close_price_expire_date:
                    return 1.0
                else:
                    return 0.0

        result['right_prediction']=result.apply(lambda row: calculate_final_gain_no_option_cost(row['option_type'], row['stock_close_price_transaction_date'], row['close_price_expire_date']), axis=1)
        result.to_csv("stock_close_price.csv")
        #print stocks

        # stocks_close_price={}
        # cursor = db.cursor()
        # for index, row in data_frame.iterrows():
        #     symbol = row['underlying_stock']
        #     sql_statement = "select close_price from eodequity where symbol='"+symbol+"' and transaction_date='"+expire_date+"'"
        #     print sql_statement
        #     cursor.execute(sql_statement)
        #     rows = cursor.fetchone()
        #     if rows is not None:
        #         stocks_close_price[symbol]=float(rows[0])

        #print stocks_close_price



        #data_frame.to_csv('option_list.csv')
        # bar_data_C = pd.Series(list(data_frame['sum(volume)']), index=data_frame['transaction_date'], name=option_type)
        #
        # option_type = 'P'
        # data_frame = sql.read_sql(
        #     sql_statement % {'begin_date': start_date_str, 'end_date': end_date_str, 'table_name': table_name,
        #                      'option_type': option_type, 'underlying_stock': underlying_stock}, db)
        # bar_data_P = pd.Series(list(data_frame['sum(volume)']), index=data_frame['transaction_date'], name=option_type)
        #
        # bar_data = pd.concat([bar_data_C, bar_data_P], axis=1)
        # bar_data.plot(kind='bar', ax=ax, title=underlying_stock + "(" + expire_date + ")")
        db.close()



    def daily_run(self):
        Config = configparser.ConfigParser()
        Config.read(self.config_file)
        running_time = datetime.datetime.now()
        self.generate_temp_option_data(Config, running_time)
        self.add_date_column_to_temp_data_file(Config, running_time)
        file_name = self.get_yahoo_data_dir(Config, running_time) + "/" + self.get_file_name(running_time)
        self.upload_csv_to_db(Config, file_name)
