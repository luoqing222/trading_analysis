__author__ = 'Qing'

import MySQLdb
import pandas as pd
import pandas.io.sql as sql

class EodDataAnalyser:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def generate_stock_closed_price_plot(self, start_date, end_date, underlying_stock, ax, color):
        ''' function to draw the plot of stock closed price between start_date and end date, used in backward testing
        :param start_date:
        :param end_date:
        :param underlying_stock:
        :param ax:
        :return:
        '''
        db = MySQLdb.connect(host=self.host, db=self.database, user=self.user, passwd=self.password)
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
        stock_data.plot(kind = 'line', ax = ax, title = underlying_stock,color = color)
        db.close()

    def generate_stock_weighted_average_price_plot(self,start_date, end_date, underlying_stock, ax, color):
        ''' function to draw the plot of stock closed price between start_date and end date, used in backward testing
        :param start_date:
        :param end_date:
        :param underlying_stock:
        :param ax:
        :return:
        '''
        db = MySQLdb.connect(host=self.host, db=self.database, user=self.user, passwd=self.password)
        table_name = "dailyvolumeweightedprice"

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        sql_statement = (
            "select transaction_date, average_price from %(table_name)s where symbol"
            "='%(underlying_stock)s' and transaction_date between '%(begin_date)s' and '%(end_date)s' "
            "order by transaction_date asc")

        data_frame = sql.read_sql(
            sql_statement % {'begin_date': start_date_str, 'end_date': end_date_str, 'table_name': table_name, 'underlying_stock': underlying_stock}, db)
        stock_data = pd.Series(list(data_frame['average_price']),index = data_frame['transaction_date'])
        stock_data.plot(kind = 'line', ax = ax, title = underlying_stock,color = color)
        db.close()

    def generate_stock_bar_volume_bar(self, start_date, end_date, underlying_stock, ax):
        ''' function to draw the bar of stock trading activity using daily bar data. Y- axis is the volume and X-axis is the close price
        :param start_date:
        :param end_date:
        :param underlyging_stock:
        :param ax:
        :param color:
        :return:
        '''
        db = MySQLdb.connect(host=self.host, db=self.database, user=self.user, passwd=self.password)
        table_name = "bar1mineoddata"

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        sql_statement = (
            "select close_price, volume from %(table_name)s where symbol"
            "='%(underlying_stock)s' and transaction_date between '%(begin_date)s' and '%(end_date)s' "
            "order by close_price asc")

        data_frame = sql.read_sql(
            sql_statement % {'begin_date': start_date_str, 'end_date': end_date_str, 'table_name': table_name, 'underlying_stock': underlying_stock}, db)

        stock_data={}
        for close_price, group in data_frame.groupby('close_price'):
            stock_data[close_price*100]=sum(group['volume'])


        ax.bar(stock_data.keys(), stock_data.values(),align='center')
        db.close()

    def generate_stock_volume_bar(self, start_date, end_date, underlying_stock, ax):
        db = MySQLdb.connect(host=self.host, db=self.database, user=self.user, passwd=self.password)
        table_name = "eodequity"

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        sql_statement = (
            "select transaction_date, volume from %(table_name)s where symbol"
            "='%(underlying_stock)s' and transaction_date between '%(begin_date)s' and '%(end_date)s' "
            "order by transaction_date asc")

        data_frame = sql.read_sql(
            sql_statement % {'begin_date': start_date_str, 'end_date': end_date_str, 'table_name': table_name, 'underlying_stock': underlying_stock}, db)

        db.close()

        stock_data = pd.Series(list(data_frame['volume']), index=data_frame['transaction_date'], name='volume')
        stock_data.plot(kind='bar', ax=ax, title=underlying_stock)





