import timeit
import pandas as pd

from mda import Mda
from simulator import data_generator


class SimulatorMda(Mda):

    def __init__(self):
        super().__init__()

    def start(self):
        data_generator.generate(self.tick)

    def tick(self, data):
        start = timeit.default_timer()
        df = pd.json_normalize(data)
        df = df.apply(self.convert_bid_ask_to_columns, axis=1)
        df = df.drop(['bids', 'asks', 'instrument'], axis=1)
        df.to_sql(data['symbol'], con=self.engine, if_exists='append')
        # super().save(data['symbol'], df)
        stop = timeit.default_timer()
        execution_time = stop - start
        print('write Time: ' + str(execution_time))

    @staticmethod
    def convert_bid_ask_to_columns(df):
        for idx in range(0, 5):
            prefix = 'bid' + str(idx)
            df[prefix + '_qty'] = df['bids'][idx]['quantity']
            df[prefix + '_price'] = df['bids'][idx]['price']
            df[prefix + '_orders'] = df['bids'][idx]['orders']

            prefix = 'ask' + str(idx)
            df[prefix + '_qty'] = df['asks'][idx]['quantity']
            df[prefix + '_price'] = df['asks'][idx]['price']
            df[prefix + '_orders'] = df['asks'][idx]['orders']
        return df


simulator_mda = SimulatorMda()
simulator_mda.start()