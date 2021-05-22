import configparser
import os

import pandas as pd
import sqlalchemy
import timeit
from app import App


class Mdc(App):

    def __init__(self):
        super().__init__()
        self.db_type = self.config['Mdc']['DbType']
        self.db_path = self.config.get('Mdc', 'DbPath', vars=os.environ)
        self.logger.info("db_type: {}, db_path={}".format(self.db_type, self.db_path))

        if self.db_type == 'mysql':
            self.engine = sqlalchemy.create_engine(self.db_path, echo=False)
        elif self.db_type == 'sqllite':
            self.engine = sqlalchemy.create_engine(self.db_path, echo=False, poolclass=sqlalchemy.pool.QueuePool,
                                                   connect_args={"check_same_thread": False})
        else:
            raise str("No db.type specified")

    def start(self):
        pass

    def tick(self, data):
        pass

    def save(self, symbol, df):
        # start = timeit.default_timer()
        df.to_sql(symbol, con=self.engine, if_exists='append', index=False)
        # stop = timeit.default_timer()
        # execution_time = stop - start
        # self.logger.debug('DB write Time: {} secs'.format(str(execution_time)))
        # try:
        #     # this will fail if there is a new column
        #     df.to_sql(symbol, con=self.engine, if_exists='append')
        # except:
        #     self.logger.debug('Adding New Columns for '.format(symbol))
        #     data = pd.read_sql("SELECT * FROM '{}'".format(symbol), con=self.engine)
        #     df2 = pd.concat([data, df])
        #     df2['dateTime'] = pd.to_datetime(df2['dateTime'], errors='coerce')
        #     df2.drop_duplicates(keep='last', inplace=True, subset=['dateTime'])
        #     df2.to_sql(symbol, con=self.engine, if_exists='replace', index=False)
