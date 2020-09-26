import configparser
import pandas as pd
import sqlalchemy


class Mda(object):

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(self.__class__.__name__ + '.ini')

        self.db_type = self.config['Mda']['db_type']
        self.db_path = self.config['Mda']['db_path']
        self.subscribe_symbol_table = self.config['Mda']['subscribe_symbol_table']

        if self.db_type == 'mysql':
            self.engine = sqlalchemy.create_engine(self.db_path, echo=False)
        elif self.db_type == 'sqllite':
            self.engine = sqlalchemy.create_engine(self.db_path, echo=False)
        else:
            raise str("No db_type specified")

        try:
            self.subscribe_symbols = pd.read_sql('SELECT * FROM {}'.format(self.subscribe_symbol_table), con=self.engine)
        except:
            print('Error while reading subscription db')
            self.subscribe_symbols = pd.DataFrame()


    def start(self):
        pass

    def tick(self, data):
        pass

    def save(self, symbol, df):
        df.to_sql(symbol, con=self.engine, if_exists='append')

