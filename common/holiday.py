import pandas as pd
import datetime
from datetime import date

holiday_df = pd.read_csv("../data/exchange-holidays.csv")
holiday_df['Date'] = pd.to_datetime(holiday_df['Date'], errors='coerce', format='%d-%b-%Y')

if any(holiday_df.Date == (pd.to_datetime('today').normalize())):
    print("True")
    exit(1)
else:
    print("False")
    exit(0)

