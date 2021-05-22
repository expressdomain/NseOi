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
        self.logger.info("Application initialized")

    def start(self):
        super().start()
        subscribe_thread = threading.Thread(target=self.subscribe)
        subscribe_thread.start()

        # self.upstox.subscribe(self.upstox.get_instrument_by_symbol('MCX_FO', 'gold20octfut'), LiveFeedType.Full)
        self.upstox.set_on_quote_update(self.tick)
        self.upstox.start_websocket(True)

        self.logger.info("Application started")
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
        self.logger.debug("Write Time: {}".format(execution_time))

    def subscribe(self):
        self.upstox.get_master_contract('NSE_FO')
        subscription_list = pd.read_csv(self.config['Mda-Upstox']['SubscriptionFile'])
        for index, row in subscription_list.iterrows():
            status = self.upstox.subscribe(self.upstox.get_instrument_by_symbol(row['exchange'], row['symbol']),
                                           LiveFeedType.Full)
            self.logger.debug("subscribe: {}".format(status))
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
