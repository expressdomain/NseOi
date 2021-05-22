from datetime import datetime


def stringToDate(dateString, dateFormat):
    return datetime.strptime(dateString, dateFormat)
