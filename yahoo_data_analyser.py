__author__ = 'qingluo'

import datetime
import models
from scipy import stats

import trading_date_utility
import dictionary_ids


class YahooEquityDataAnalyser:
    def __init__(self):
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

        #write the header
        #file.write("date,symbol,")
        #for index in benchmarks:
        #    file.write(index+",")

        #file.write("\n")

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








