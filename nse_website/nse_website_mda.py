import random
import threading
import timeit
import pandas as pd

from mda import Mda
from simulator import data_generator
from utils.common import stringToDate
from utils.proxy_request import ProxyRequests


class NseWebsiteMda(Mda):

    def __init__(self):
        super().__init__()
        self.proxyRequest = ProxyRequests()
        response = self.proxyRequest.get("https://www.nseindia.com")
        self.query_delay = self.config.getfloat('Mda-NseWebsite', 'QueryDelay')
        self.expiryList = self.config['Mda-NseWebsite']['ExpiryList'].split(',')

    def start(self):
        t1 = threading.Thread(target=self.schedule_query, args=(
            self.query_delay, 'fut', 'https://www.nseindia.com/api/liveEquity-derivatives?index=nse50_fut'))
        t1.start()
        t2 = threading.Thread(target=self.schedule_query, args=(
            self.query_delay, 'fut', 'https://www.nseindia.com/api/liveEquity-derivatives?index=nifty_bank_fut'))
        t2.start()
        t3 = threading.Thread(target=self.schedule_query,
                              args=(self.query_delay, 'opt',
                                    'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'))
        t3.start()

        t4 = threading.Thread(target=self.schedule_query,
                              args=(self.query_delay, 'opt',
                                    'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'))
        t4.start()

    def tick(self, data):
        pass

    def query_fut_data(self, url):
        def save_row(row):
            row_df = row.to_frame().T[
                ['dateTime', 'lastPrice', 'change', 'volume', 'underlyingValue', 'openInterest',
                 'noOfTrades']]
            self.save(row['identifier'][:-6], row_df)

        try:
            print("Querying " + url)
            response = self.proxyRequest.get(url)
            resJson = response.json()
            timeStamp = stringToDate(resJson['timestamp'], '%d-%b-%Y %H:%M:%S')

            df = pd.json_normalize(resJson['data'])
            df['dateTime'] = timeStamp
            df = df.drop(['meta.data', 'meta.msg'], axis=1)
            df.apply(save_row, axis=1)
        except Exception as e:
            print(str(e))
            pass

    def query_opt_data(self, url):
        def save_row(row):
            row_df = row.to_frame().T[
                ['dateTime', 'openInterest', 'totalTradedVolume',
                 'impliedVolatility', 'lastPrice', 'totalBuyQuantity', 'totalSellQuantity',
                 'bidQty', 'bidprice', 'askQty', 'askPrice', 'underlyingValue']]

            self.save(row['identifier'][:-3], row_df)

        try:
            print("Querying " + url)
            response = self.proxyRequest.get(url)
            resJson = response.json()
            timeStamp = stringToDate(resJson['records']['timestamp'], '%d-%b-%Y %H:%M:%S')

            start = timeit.default_timer()
            for item in resJson['records']['data']:
                if item['expiryDate'] in self.expiryList:
                    if 'CE' in item:
                        ce_df = pd.json_normalize(item['CE'])
                        ce_df['dateTime'] = timeStamp
                        ce_df.apply(save_row, axis=1)
                    if 'PE' in item:
                        pe_df = pd.json_normalize(item['PE'])
                        pe_df['dateTime'] = timeStamp
                        pe_df.apply(save_row, axis=1)

            stop = timeit.default_timer()
            execution_time = stop - start
            print('Program Executed in ' + str(execution_time))
        except Exception as e:
            print(str(e))
            pass

    def schedule_query(self, duration, instrument_type, url):
        random_duration = random.randint(duration, duration + 10)
        print("delay = {}".format(random_duration))
        if instrument_type == 'fut':
            self.query_fut_data(url)
        elif instrument_type == 'opt':
            self.query_opt_data(url)
        threading.Timer(random_duration, self.schedule_query, [duration, instrument_type, url]).start()


nse_website_mda = NseWebsiteMda()
nse_website_mda.start()
