import os

import pandas as pd
import numpy as np
import datetime
from datetime import date

def get_expires(start="2020-10-01", end="2021-12-31"):
    # get holidays
    parent_path = os.getenv('PYTHONPATH')
    # holiday_df = pd.read_csv("{}/data/exchange-holidays.csv".format(parent_path))
    holiday_df = pd.read_csv("/home/rtalokar/win-d/work/stock_market/data/exchange-holidays.csv".format(parent_path))
    holiday_df['Date'] = pd.to_datetime(holiday_df['Date'], errors='coerce', format='%d-%b-%Y')

    # calculate expires
    expiry_df = pd.DataFrame(columns=['date'])
    for d in pd.date_range(start=start, end=end):
        if d.weekday() == 3:
            while any(holiday_df.Date == d):
                d = d - pd.Timedelta(days=1)
            expiry_df = expiry_df.append({'date': d}, ignore_index=True)
            # expiry_list.append(d.strftime('%d-%b-%Y'))
    return expiry_df

def get_historical_future_expires(start="2020-10-01", end="2021-12-31"):
    expiry_df = get_expires(start=start, end=end)
    today = pd.to_datetime('today').normalize()
    historical_df = expiry_df[expiry_df['date'] < today].\
        apply(lambda date: date.dt.strftime('%d-%b-%Y'))['date'].to_list()
    future_df = expiry_df[expiry_df['date'] >= today].\
        apply(lambda date: date.dt.strftime('%d-%b-%Y'))['date'].to_list()
    # print(future_df)
    return historical_df, future_df

def get_symbol_list():
    return ['NIFTY', 'BANKNIFTY']

def get_expiry_list():
    return ['26-11-2020', '03-12-2020', '10-12-2020', '17-12-2020', '24-12-2020', '31-12-2020']

def get_strike_list(symbol):
    if symbol == 'NIFTY':
        return np.arange(13000, 16000, 50)
    elif symbol == 'BANKNIFTY':
        return np.arange(26000, 40000, 100)
    else:
        return None

def get_timeframe_list():
    return np.arange(5, 65, 5)


# get_historical_future_expires()