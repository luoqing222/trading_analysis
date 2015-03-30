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
    def calculate_daily_rsq(symbols, benchmarks, current_date, time_window, file):
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
            # print symbol
            # print symbol_return[symbol]
        for symbol in symbols:
            print "calculating R-square for "+symbol
            x_symbol=symbol_return[symbol]
            file.write(symbol+",")
            for benchmark in benchmarks:
                y_benchmark=benchmark_return[benchmark]
                #run the linear regression to calculate r-square
                slope, intercept, r_value, p_value, std_err = stats.linregress(x_symbol,y_benchmark)
                file.write(str(r_value**2))
                file.write(",")
            file.write("\n")


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



