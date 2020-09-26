import timeit
import pandas as pd
from upstox_api.api import *

from mda import Mda


class UpstoxMda(Mda):

    def __init__(self):
        super().__init__()
        self.api_key = self.config['Mda-Upstox']['api_key']
        self.access_token = self.config['Mda-Upstox']['access_token']
        self.upstox = Upstox(self.api_key, self.access_token)
        self.nse_fo_master = self.upstox.get_master_contract('NSE_FO')

    def start(self):
        super().start()
        self.upstox.set_on_quote_update(self.tick)
        self.subscribe_symbols.apply(lambda symbol: self.upstox.subscribe(
            self.upstox.get_instrument_by_symbol(symbol['exchange'], symbol['gold20octfut']), LiveFeedType.Full),
                                     axis=1)
        self.upstox.start_websocket(True)

        condition = threading.Condition()
        condition.acquire()
        condition.wait()

    def tick(self, data):
        start = timeit.default_timer()
        df = pd.json_normalize(data)
        df = df.apply(self.convert_bid_ask_to_columns, axis=1)
        df = df.drop(['bids', 'asks', 'instrument'], axis=1)
        super().save(data['symbol'], df)
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
