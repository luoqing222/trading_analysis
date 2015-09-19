__author__ = 'Qing'

import MySQLdb
import pandas as pd
import pandas.io.sql as sql
import datetime

class YahooOptionAnalyser:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def generate_option_sum_bar(self, start_date, end_date, underlying_stock, ax):
        ''' function to generate the bar plot for underlying_stock
        :param start_date: start date
        :param end_date:
        :param underlying_stock:
        :param ax:
        :param expire_date: Only for illustration purpose, not essential in the calculation, can be any value
        :return:
        '''
        db = MySQLdb.connect(host=self.host, db=self.database, user=self.user, passwd=self.password)
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
        bar_data.plot(kind='bar', ax=ax, title=underlying_stock)
        db.close()

    def filter_stock_by_option_volume(self, analysis_date, date_window, filter_parameter):
        ''' function to select the contract that has significant volume change
        :param analysis_date: the date that is doing analysis
        :param date_window: the date window as the bench mark
        :param filter_parameter: the parameters
        :return: stock list
        '''
        db = MySQLdb.connect(host=self.host, db=self.database, user=self.user, passwd=self.password)
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

    def find_contracts_with_significant_volume_change(self, analysis_date, num_of_days_before_analysis,filter_parameter):
        running_date = analysis_date
        date_window = []
        for day in range(1, num_of_days_before_analysis):
            date_window.append(running_date + datetime.timedelta(days=-day))

        result_table = self.filter_stock_by_option_volume(running_date, date_window, filter_parameter)
        return result_table

