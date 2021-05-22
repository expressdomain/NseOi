import sqlalchemy
import pandas as pd
from sqlalchemy import inspect
from datetime import datetime, timedelta

read_engine = sqlalchemy.create_engine('sqlite:////mnt/hgfs/win-d/work/data/20210220-nse.db', echo=False)
write_engine = sqlalchemy.create_engine('sqlite:////mnt/hgfs/win-d/work/data/20210220-nse-cleaned.db', echo=False)

inspector = inspect(read_engine)

total_tables = len(inspector.get_table_names())
# bday = pd.tseries.offsets.CustomBusinessDay(holidays=holidays, weekmask=weekmask)

def calculate_change_in_oi(df):
    try:
        return df[df['dateTime'] == (df['dateTime'][0] - timedelta(days=1))]['openInterest'].iloc[0]
    except:
        return 0


for idx, table_name in enumerate(inspector.get_table_names()):
    print("Processing: {}/{} - {}".format(idx, total_tables, table_name))
    df = pd.read_sql("SELECT * FROM '{}'".format(table_name), con=read_engine)
    df['dateTime'] = pd.to_datetime(df['dateTime'], errors='coerce')
    df.drop_duplicates(keep='first', inplace=True, subset=['dateTime'])
    # df.drop(['index'], inplace=True, axis=1)
    df.to_sql(table_name, con=write_engine, if_exists='replace', index=False)
