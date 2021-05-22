import sqlalchemy

from app import App


class Mda(App):

    def __init__(self):
        super().__init__()
        self.db_type = self.config['Mda']['DbType']
        self.db_path = self.config['Mda']['DbPath']

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
        df.to_sql(symbol, con=self.engine, if_exists='append')
