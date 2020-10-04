import time
import pandas as pd
import sqlalchemy

engine = sqlalchemy.create_engine('sqlite:///data/nse.db', echo=False, poolclass=sqlalchemy.pool.QueuePool,
                                                   connect_args={"check_same_thread": False})

pd.read_sql("SELECT * FROM 'OPTIDXNIFTY01-10-2020PE11300'", con=engine)
pd.read_sql("SELECT * FROM 'FUTIDXNIFTY29-10-2020'", con=engine)
pd.read_sql("SELECT * FROM 'FUTIDXNIFTY26-11-2020'", con=engine)

df = pd.read_sql("SELECT * FROM 'OPTIDXNIFTY29-10-2020PE11400'", con=engine)
df['dateTime'] = pd.to_datetime(df['dateTime'], errors='coerce')
df.resample('30T', on='dateTime').first()
def read_hdf():
    df = pd.read_sql("SELECT * FROM 'OPTIDXNIFTY31-12-2020PE11300'", con=engine)
    return df

read_hdf()


