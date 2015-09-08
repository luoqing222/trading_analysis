__author__ = 'Qing'

import trading_data_utility_by_sql
import datetime
import configparser


if __name__ == "__main__":
    config_file = "option_data_management_setting.ini"

    config = configparser.ConfigParser()
    config.read(config_file)
    host = config.get("database", "host")
    database = config.get("database", "database")
    user = config.get("database", "user")
    password = config.get("database", "passwd")

    data_utility = trading_data_utility_by_sql.TradingDataUtilityBySQL(host, database, user,password)
    running_time = datetime.datetime.now()
    #running_time = datetime.datetime(year=2015, month=9, day=06)
    print data_utility.get_nasdaq_list(running_time)


