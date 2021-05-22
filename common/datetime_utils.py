import datetime


def convert_date(in_datetime, from_format, to_format):
    return datetime.datetime.strptime(in_datetime, from_format).strftime(to_format)
