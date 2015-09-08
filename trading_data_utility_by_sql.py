__author__ = 'Qing'

import MySQLdb
import re


class TradingDataUtilityBySQL:
    def __init__(self, host, database, user, password):
        self.conn = MySQLdb.connect(host=host,db=database, user=user, passwd=password)

    def get_nyse_list(self, running_time):
        '''
        :param running_time: the time stample in the table eodequity
        :return:  NYSE symbol list that in the table
        '''
        cursor = self.conn.cursor()
        sql_statement = "select distinct(symbol) from eodequity where transaction_date in (select max(transaction_date)" \
                        "from eodequity where transaction_date<= \"" + running_time.strftime("%Y-%m-%d") + "\") and exchange = 'NYSE'"
        cursor.execute(sql_statement)
        rows = cursor.fetchall()
        result = []
        p = re.compile('[a-z]*[A-Z]*\-[a-z]*[A-Z]*')
        for row in rows:
            if not p.match(row[0]):
                result.append(row[0])
        return result

    def get_nasdaq_list(self, running_time):
        '''
        :param runnning_time: the time stample in the table eodequity
        :return: NASDAQ symbol list that in the table
        '''
        cursor = self.conn.cursor()
        sql_statement = "select distinct(symbol) from eodequity where transaction_date in (select max(transaction_date)" \
                        "from eodequity where transaction_date<= \"" + running_time.strftime("%Y-%m-%d") + "\") and exchange = 'NASDAQ'"
        cursor.execute(sql_statement)
        rows = cursor.fetchall()
        result = []
        p = re.compile('[a-z]*[A-Z]*\-[a-z]*[A-Z]*')
        for row in rows:
            if not p.match(row[0]):
                result.append(row[0])
        return result



