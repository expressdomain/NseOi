import time
import timeit
import pandas as pd
import threading
from upstox_api.api import *

from mda import Mda


class UpstoxMda(Mda):

    def __init__(self):
        super().__init__()
        self.ApiKey = self.config['Mda-Upstox']['ApiKey']
        self.AccessToken = self.config['Mda-Upstox']['AccessToken']
        self.upstox = Upstox(self.ApiKey, self.AccessToken)
        self.nse_fo_master = self.upstox.get_master_contract('NSE_FO')

    def start(self):
        super().start()
        subscribe_thread = threading.Thread(target=self.subscribe)
        subscribe_thread.start()

        self.upstox.set_on_quote_update(self.tick)
        # self.subscribe_symbols.apply(lambda symbol: self.upstox.subscribe(
        #     self.upstox.get_instrument_by_symbol('NSE_FO', 'nifty20100110000ce'), LiveFeedType.Full),
        #                              axis=1)
        self.upstox.start_websocket(True)

        condition = threading.Condition()
        condition.acquire()
        condition.wait()

    def tick(self, data):
        # print(data)
        # return
        start = timeit.default_timer()
        df = pd.json_normalize(data)
        df = df.apply(self.convert_bid_ask_to_columns, axis=1)
        df = df.drop(['bids', 'asks', 'instrument'], axis=1)
        super().save(data['symbol'], df)
        stop = timeit.default_timer()
        execution_time = stop - start
        print('write Time: ' + str(execution_time))

    def subscribe(self):
        subscription_list = pd.read_csv(self.config['Mda-Upstox']['SubscriptionFile'])
        for index, row in subscription_list.iterrows():
            print(self.upstox.subscribe(self.upstox.get_instrument_by_symbol(row['exchange'], row['symbol']),
                                        LiveFeedType.Full))
            time.sleep(self.config.getfloat('Mda-Upstox', 'SubscriptionDelay'))

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


upstox_mda = UpstoxMda()
upstox_mda.start()
