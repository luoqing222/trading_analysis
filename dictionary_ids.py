__author__ = 'qingluo'


class DailyTickerID:
    def __init__(self, name, date):
        self.name = name
        self.date = date

    def __hash__(self):
        return hash((self.name, self.date))

    def __eq__(self, other):
        return (self.name, self.date) == (other.name, other.date)
