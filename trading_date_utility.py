__author__ = 'qingluo'

import pandas as pd
import datetime
import models
import sets
import peewee

# find the first available trading day before date
def prev_business_day(date, country):
    '''
    :param date: the date object
    :param country: default is "US"
    :return: the date object that is the previous business date
    '''
    next_day = pd.to_datetime(date) + pd.DateOffset(days=-1)
    (year, week, weekday) = next_day.isocalendar()
    year, month, day = next_day.year, next_day.month, next_day.day
    holiday = models.HolidayCalendar.select().where((models.HolidayCalendar.date.year == year)
                                                    & (models.HolidayCalendar.date.month == month)
                                                    & (models.HolidayCalendar.date.day == day)
                                                    & (models.HolidayCalendar.country_code == country))

    while (weekday > 5) or holiday.count() != 0:
        next_day = pd.to_datetime(next_day) + pd.DateOffset(days=-1)
        (year, week, weekday) = next_day.isocalendar()
        year, month, day = next_day.year, next_day.month, next_day.day
        holiday = models.HolidayCalendar.select().where((models.HolidayCalendar.date.year == year)
                                                        & (models.HolidayCalendar.date.month == month)
                                                        & (models.HolidayCalendar.date.day == day)
                                                        & (models.HolidayCalendar.country_code == country))

    return datetime.datetime(year, month, day).date()


# find the next available trading date after date
def next_business_day(date, country):
    '''
    :param date: date object
    :param country: default is "US"
    :return: the date object that is the next business day
    '''
    next_day = pd.to_datetime(date) + pd.DateOffset(days=1)
    (year, week, weekday) = next_day.isocalendar()
    year, month, day = next_day.year, next_day.month, next_day.day
    holiday = models.HolidayCalendar.select().where((models.HolidayCalendar.date.year == year)
                                                    & (models.HolidayCalendar.date.month == month)
                                                    & (models.HolidayCalendar.date.day == day)
                                                    & (models.HolidayCalendar.country_code == country))

    while (weekday > 5) or holiday.count() != 0:
        next_day = pd.to_datetime(next_day) + pd.DateOffset(days=1)
        (year, week, weekday) = next_day.isocalendar()
        year, month, day = next_day.year, next_day.month, next_day.day
        holiday = models.HolidayCalendar.select().where((models.HolidayCalendar.date.year == year)
                                                        & (models.HolidayCalendar.date.month == month)
                                                        & (models.HolidayCalendar.date.day == day)
                                                        & (models.HolidayCalendar.country_code == country))

    return datetime.datetime(year, month, day).date()


def next_trading_day(date, country, delta):
    current_date = date
    if delta > 0:
        for i in range(0, delta):
            current_date = next_business_day(current_date, country)
    elif delta < 0:
        for i in range(0, -delta):
            current_date = prev_business_day(current_date, country)

    return current_date


# function to determine if the date in the country is trading date or not
#if it is trading date, return True; otherwise return False
#For example, is_trading_day("03/29/2015", "US") will return False
#is_trading_day("03/30/2015, "US") will return True
def is_trading_day(date, country):
    (year, week, weekday) = date.isocalendar()
    # first check the weekday, not trading day if Saturday or Sunday
    if weekday > 5:
        return False
    # then check if it is national holiday
    year = date.year
    month = date.month
    day = date.day

    holiday = models.HolidayCalendar.select().where((models.HolidayCalendar.date.year == year)
                                                    & (models.HolidayCalendar.date.month == month)
                                                    & (models.HolidayCalendar.date.day == day)
                                                    & (models.HolidayCalendar.country_code == country))

    if holiday.count() != 0:
        return False

    return True


    #function to determine if the date in the country is trading date or not
    #if it is trading date, return True; otherwise return False
    #For example, is_trading_day("03/29/2015", "US") will return False
    #is_trading_day("03/30/2015, "US") will return True
def nearest_trading_day(date, country):
    '''
    :param date: date object
    :param country: default is "US"
    :return: date object
    '''
    if is_trading_day(date, country):
        return date
    return prev_business_day(date, country)


#function to get the dates with trading date for symbol in the country
def data_available_dates(start_date, end_date, symbol):
    result = []
    data_record = models.HistoricalPrice.select().where((models.HistoricalPrice.symbol == symbol)
                                                        & (models.HistoricalPrice.transaction_date >= start_date)
                                                        & (
                                                            models.HistoricalPrice.transaction_date <= end_date)).order_by(
        models.HistoricalPrice.transaction_date)

    for i in range(0, data_record.count() - 1):
        result.append(data_record[i].transaction_date)

    return result


def generate_previous_trading_date_dict(start_date, end_date, max_move_days):
    result = {}
    country = "US"
    extended_start_date = nearest_trading_day(start_date + datetime.timedelta(days=-max_move_days), country)
    extended_end_date = nearest_trading_day(end_date + datetime.timedelta(days=max_move_days), country)
    while extended_end_date > extended_start_date:
        pre_date = prev_business_day(extended_end_date, country)
        result[extended_end_date] = pre_date
        extended_end_date = pre_date
    return result


def previous_n_trading_days(trading_date, n, trading_date_map):
    start_date = nearest_trading_day(trading_date,"US")
    for i in range(0, n):
        start_date = trading_date_map[start_date]
    return start_date

def delete_holiday(year, month, day, country):
    models.db.connect()

    holiday = datetime.datetime(year, month, day)
    query = models.HolidayCalendar.delete().where(models.HolidayCalendar.date == holiday,
                                                      models.HolidayCalendar.country_code == country)
    query.execute()

def add_holiday(year, month, day, country):
    models.db.connect()
    if not models.HolidayCalendar.table_exists():
        models.db.create_table(models.HolidayCalendar)

    holiday = datetime.datetime(year, month, day)
    models.HolidayCalendar.create(date=holiday, country_code=country)


#function to populate the holiday into database table
def populate_holiday():
    models.HolidayCalendar.drop_table()
    models.db.create_table(models.HolidayCalendar)
    add_holiday(2015, 4, 3, "US")
    add_holiday(2015, 5, 25, "US")
    add_holiday(2015, 7, 3, "US")
    add_holiday(2015, 9, 7, "US")
    add_holiday(2015, 11, 26, "US")
    add_holiday(2015, 11, 27, "US")
    add_holiday(2015, 12, 25, "US")

#function to populate the historical holiday into database table
#rule: is the dates has SPY data, we consider it as trading date, spy data staring from 1993/01/29
def populate_historical_holiday():
    trading_dates = sets.Set([])
    start_date = "1983/01/01"
    update_date = "2015/05/09"
    symbol = "spy"
    data_record = models.HistoricalPrice.select().where((models.HistoricalPrice.symbol == symbol)
                                                            & (models.HistoricalPrice.transaction_date >= start_date)
                                                            & (models.HistoricalPrice.transaction_date <= update_date)).order_by(models.HistoricalPrice.transaction_date)
    #print data_record.count()
    for i in range(0, data_record.count()):
        trading_dates.add(data_record[i].transaction_date.toordinal())
    for date in range(min(trading_dates), max(trading_dates)):
        if date not in trading_dates:
            nonordinal_date = datetime.date.fromordinal(date)
            (year, week, weekday) = nonordinal_date.isocalendar()
            if weekday < 6:
                print nonordinal_date.year, nonordinal_date.month, nonordinal_date.day
                try:
                    add_holiday(nonordinal_date.year, nonordinal_date.month, nonordinal_date.day,
                                                       "US")
                except peewee.IntegrityError:
                    pass






