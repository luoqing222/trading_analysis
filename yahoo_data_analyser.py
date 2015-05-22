__author__ = 'qingluo'

import datetime
import models
from scipy import stats
import os

import trading_date_utility
import dictionary_ids
import numpy
import MySQLdb


class YahooEquityDataAnalyser:
    def __init__(self,db):
        self.db=db
        pass

    # this function is to calculate the rsq of symbol to the benchmark based
    # data entry staring from current date and move backwards in time_window days
    #for example calculate_daily_rsq("FB", "ivv", "2/28/2015", 30) is to calculate the
    #rsquare of FB to index on 02/28/2015 and the sample data takes 30 days
    @staticmethod
    def calculate_daily_rsq(symbols, benchmarks, current_date, time_window, file_name):
        file = open(file_name, "w")
        file.write("symbol,")
        for index in benchmarks:
            file.write(index+",")

        file.write("\n")

        current_date = trading_date_utility.nearest_trading_day(datetime.datetime.now(), "US")
        dates_window = []
        for i in range(0, time_window+1):
            dates_window.append(trading_date_utility.next_trading_day(current_date, "US", -i))

        benchmark_return={}
        for benchmark in benchmarks:
            print "calculating return for "+benchmark
            benchmark_return[benchmark] = YahooEquityDataAnalyser.get_daily_returns(benchmark, dates_window)
            # print benchmark
            # print benchmark_return[benchmark]

        symbol_return={}
        for symbol in symbols:
            print "calculating return for "+symbol
            symbol_return[symbol] = YahooEquityDataAnalyser.get_daily_returns(symbol, dates_window)
        average_r_square={}
        total_count={}
        for benchmark in benchmarks:
            average_r_square[benchmark]=0.0
            total_count[benchmark]=0
        for symbol in symbols:
            print "calculating R-square for "+symbol
            x_symbol=symbol_return[symbol]
            file.write(symbol+",")
            for benchmark in benchmarks:
                y_benchmark=benchmark_return[benchmark]
                #run the linear regression to calculate r-square
                if len(x_symbol)==len(y_benchmark) and len(x_symbol)!=0:
                    slope, intercept, r_value, p_value, std_err = stats.linregress(x_symbol,y_benchmark)
                    file.write(str(r_value**2))
                    average_r_square[benchmark] += r_value ** 2
                    total_count[benchmark] += 1
                else:
                    file.write(str(-99.0))
                file.write(",")
            file.write("\n")

        file.write("average R-square,")
        for benchmark in benchmarks:
            file.write(str(average_r_square[benchmark]/total_count[benchmark])+",")
        file.close()


    @staticmethod
    def get_daily_returns(symbol, dates):
        daily_return_table = []
        models.db.connect()
        min_date= min(dates)
        max_date = max(dates)
        # find the symbol's adjusted price
        data_record = models.HistoricalPrice.select().where((models.HistoricalPrice.symbol == symbol)
                                                                & (models.HistoricalPrice.transaction_date >= min_date)
        & (models.HistoricalPrice.transaction_date <= max_date)).order_by(models.HistoricalPrice.transaction_date)

        for i in range(data_record.count()-1, 0, -1):
            daily_return_table.append(data_record[i].adjust_close / data_record[i-1].adjust_close - 1.0)

        return daily_return_table


    @staticmethod
    def calculate_historical_rsq(symbols, benchmarks, start_date,end_date, time_window, file_name):
        file = open(file_name, "w")

        symbol="spy"

        data_record = models.HistoricalPrice.select().where((models.HistoricalPrice.symbol == symbol)
                                        & (models.HistoricalPrice.transaction_date >= start_date)
        & (models.HistoricalPrice.transaction_date <= end_date)).order_by(models.HistoricalPrice.transaction_date)
        trading_dates= []

        #save the trading date
        for i in range(0,data_record.count()):
            trading_dates.append(data_record[i].transaction_date.toordinal())

        for trading_date in trading_dates:

            benchmark_return={}
            symbol_return={}
            dates_window = []

            current_date=datetime.datetime.fromordinal(trading_date)
            print "calculating R-square for "+ current_date.isoformat()

            for i in range(0, time_window+1):
                dates_window.append(trading_date_utility.next_trading_day(current_date, "US", -i))

            for symbol in symbols:
                #print "calculating return for "+symbol+" on "+ current_date.isoformat()
                symbol_return[symbol] = YahooEquityDataAnalyser.get_daily_returns(symbol, dates_window)

            for symbol in benchmarks:
                print "calculating return for "+symbol+" on "+ current_date.isoformat()
                benchmark_return[symbol] = YahooEquityDataAnalyser.get_daily_returns(symbol, dates_window)

            average_r_square={}
            total_count={}
            for benchmark in benchmarks:
                average_r_square[benchmark]=0.0
                total_count[benchmark]=0

            for symbol in symbols:
                #print "calculating R-square for "+symbol
                x_symbol=symbol_return[symbol]
                #file.write(current_date.isoformat()+","+symbol+",")

                for benchmark in benchmarks:
                    y_benchmark=benchmark_return[benchmark]
                #run the linear regression to calculate r-square

                    if len(x_symbol)==len(y_benchmark) and len(x_symbol)!=0:
                        slope, intercept, r_value, p_value, std_err = stats.linregress(x_symbol,y_benchmark)
                        #file.write(str(r_value**2))
                        average_r_square[benchmark] += r_value ** 2
                        total_count[benchmark] += 1
                    #else:
                        #file.write(str(-99.0))
                #file.write(",")
            #file.write("\n")

            file.write(current_date.isoformat()+",")
            for benchmark in benchmarks:
                file.write(str(average_r_square[benchmark]/total_count[benchmark])+",")
            file.write("\n")
        file.close()

    #function to calculate the n days return for symbols
    #used to calculate the strong stock
    def get_n_days_returns_rank_by_sql(self,symbols, date, trading_date_map, days_array, weight,stock_num):
        score = range(0,len(symbols))
        for i in range(0,len(symbols)):
            score[i]=0.0

        for index,n in enumerate(days_array):
            begin_date = trading_date_utility.previous_n_trading_days(date, n, trading_date_map)
            returns = self.get_returns_between_two_days(symbols, begin_date,date)
            #print symbols
            #print returns
            sorted_index = numpy.argsort(returns)
            for i in range(0,len(sorted_index)):
                score[sorted_index[i]]+=i*weight[index]
            #print sorted_index

        arr = numpy.array(score)
        strong_stock=[]
        for i in numpy.argsort(-arr)[:stock_num]:
            strong_stock.append(symbols[i])
        return strong_stock
        #return score

    def get_returns_between_two_days(self,symbols, begin_date,end_date):
        cursor = self.db.cursor()

        #find the mapping between symbol and adjust_close price
        start_date = trading_date_utility.nearest_trading_day(begin_date, "US")
        sql_statement= "select symbol,adjust_close from historicalprice where transaction_date=\""+start_date.strftime("%Y-%m-%d")+"\""
        cursor.execute(sql_statement)
        rows= cursor.fetchall()
        start_price={}
        for symbol,price in rows:
            start_price[symbol]=price

        final_date = trading_date_utility.nearest_trading_day(end_date, "US")
        sql_statement= "select symbol,adjust_close from historicalprice where transaction_date=\""+final_date.strftime("%Y-%m-%d")+"\""
        cursor.execute(sql_statement)
        rows= cursor.fetchall()
        final_price={}
        for symbol,price in rows:
            final_price[symbol]=price

        returns = []
        for symbol in symbols:
            try:
                begin=start_price[symbol]
                end=final_price[symbol]
                returns.append(end/begin - 1.0)
            except KeyError:
                returns.append(-99)

        return returns

    def get_average_between_two_days(self,symbols,begin_date,end_date):
        returns = self.get_returns_between_two_days(symbols, begin_date,end_date)
        my_list=[number for number in returns if number != -99.0]

        if len(my_list)==0:
            return -99.0
        else:
            return sum(my_list)/float(len(my_list))






















