__author__ = 'Qing'

import os
import models
import datetime
import MySQLdb
import logging
from peewee import *

logger = logging.getLogger(__name__)

class Eod1MinBarDataUploader:
    def __init__(self, host,database, user, password, folder):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.folder = folder
        self.file_lists = ['NASDAQ', 'NYSE']

    def upload_bar_1min_equity_to_db(self, host, database, user, password, file_name, folder):
        ''' function to save 1 minute frequency trading data into database
        :param config: config object
        :param file_name: file name need to start with NASDAQ or NYSE
        :param folder:
        :return: None
        '''
        des_file_name = folder + "/" + file_name
        exchange = file_name.split("_")[0]
        records = []
        if not os.path.exists(des_file_name):
            logger.warning(des_file_name+" does not exist!")
            return
        else:
            logger.info("uploading %s to database", des_file_name)
            models.db.init(host=host, database=database, user=user, passwd=password)
            models.db.connect()
            if not models.Bar1MinEodData.table_exists():
                models.db.create_table(models.Bar1MinEodData)
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

        #host = config.get("database", "host")
        #database = config.get("database", "database")
        #user = config.get("database", "user")
        #password = config.get("database", "passwd")
        db = MySQLdb.connect(host=host, db=database, user=user, passwd=password)
        cursor = db.cursor()
        sql_statement = "insert into bar1mineoddata(symbol,transaction_date,transaction_time, open_price, high_price,low_price, close_price,volume, exchange) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cursor.executemany(sql_statement, records)
            db.commit()
            logger.info("%s is successfully uploaded", des_file_name)
        except  Exception, e:
            logger.warning("exception is thrown when upload "+des_file_name+":"+str(e))
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()

    def run(self, running_time):
        des_folder = self.folder+ "/daily_run/" + running_time.strftime('%Y_%m_%d')+"/eod"
        if os.path.exists(des_folder):
            for file_name in self.file_lists:
                des_file_name= file_name + "_BAR_1MIN_"+ running_time.strftime('%Y%m%d') +".csv"
                self.upload_bar_1min_equity_to_db(self.host, self.database, self.user, self.password, des_file_name, des_folder)

    def historical_run(self, start_datetime,end_datetime):
        start = start_datetime
        while start < end_datetime:
            print "upload eod 1min bar data on "+start.strftime("%Y%m%d")
            self.run(start)
            start = start + datetime.timedelta(days=1)