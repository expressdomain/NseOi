import json
import random
import threading
import time
import timeit

import pandas as pd
from loguru import logger

from common import utils
from market_data.utils.common import stringToDate
from market_data.utils.proxy_request import ProxyRequests

from market_data.mdc import Mdc


class NseWebsiteMdc(Mdc):
    def __init__(self):
        super().__init__()
        self.proxyRequest = ProxyRequests()
        response = self.proxyRequest.get("https://www.nseindia.com")
        self.query_delay = self.config.getfloat('Mdc-NseWebsite', 'QueryDelay')
        self.max_expires = self.config.getint('Mdc-NseWebsite', 'MaxExpires')
        self.historicalExpiryList, self.expiryList = utils.get_historical_future_expires()
        self.expiryList = self.expiryList[:self.max_expires]
        self.logger.info("expiryList: {}".format(self.expiryList))

    def start(self):
        while True:
            url = 'https://www.nseindia.com/api/option-chain-indices?symbol={}'
            self.query_opt_data(url.format('NIFTY'))
            # time.sleep(random.randint(8, 15))
            self.query_opt_data(url.format('BANKNIFTY'))
            time.sleep(random.randint(self.query_delay, self.query_delay + 10))

        # self.schedule_query(self.query_delay, 'fut',
        #                    'https://www.nseindia.com/api/liveEquity-derivatives?index=nse50_fut')
        # self.schedule_query(self.query_delay, 'fut',
        #                    'https://www.nseindia.com/api/liveEquity-derivatives?index=nifty_bank_fut')
        # self.schedule_query(self.query_delay, 'opt',
        #                     'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY')
        # self.schedule_query(self.query_delay, 'opt',
        #                     'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY')

    def tick(self, data):
        pass

    def query_fut_data(self, url):
        def save_row(row):
            row_df = row.to_frame().T[
                ['dateTime', 'lastPrice', 'change', 'volume', 'underlyingValue', 'openInterest',
                 'noOfTrades']]
            self.save(row['identifier'][:-6], row_df)

        try:
            self.logger.debug("Requesting url: " + url)
            response = self.proxyRequest.get(url)
            if response.status_code == 200:
                self.logger.debug("Response received for: " + url)
                resJson = response.json()
                timeStamp = stringToDate(resJson['timestamp'], '%d-%b-%Y %H:%M:%S')

                df = pd.json_normalize(resJson['data'])
                df['dateTime'] = timeStamp
                df = df.drop(['meta.data', 'meta.msg'], axis=1)
                df.apply(save_row, axis=1)
        except Exception as e:
            response = self.proxyRequest.get("https://www.nseindia.com")
            self.logger.critical("Exception: " + str(e))

    def query_opt_data(self, url):
        def save_df(df):
            self.save(df['identifier'].values[0][:-3],
                      df[['dateTime', 'openInterest', 'totalTradedVolume',
                          'impliedVolatility', 'lastPrice', 'totalBuyQuantity', 'totalSellQuantity',
                          'bidQty', 'bidprice', 'askQty', 'askPrice', 'underlyingValue', 'changeinOpenInterest']])

        def save_row(row):
            row_df = row.to_frame().T[
                ['dateTime', 'openInterest', 'totalTradedVolume',
                 'impliedVolatility', 'lastPrice', 'totalBuyQuantity', 'totalSellQuantity',
                 'bidQty', 'bidprice', 'askQty', 'askPrice', 'underlyingValue', 'changeinOpenInterest']]

            self.save(row['identifier'][:-3], row_df)

        try:
            self.logger.debug("Requesting url: " + url)
            response = self.proxyRequest.get(url)
            if response.status_code == 200:
                self.logger.debug("Response received for: " + url)
                resJson = response.json()
                with open('/mnt/hgfs/win-d/work/data/json-data/json-05-02-2021-14_20_09.json') as json_file:
                    resJson = json.load(json_file)

                timeStamp = stringToDate(resJson['records']['timestamp'], '%d-%b-%Y %H:%M:%S')

                start = timeit.default_timer()
                average_write_time_per_row = 0
                for item in resJson['records']['data']:
                    if item['expiryDate'] in self.expiryList:
                        start1 = timeit.default_timer()
                        if 'CE' in item:
                            ce_df = pd.json_normalize(item['CE'])
                            ce_df['dateTime'] = timeStamp
                            # ce_df.apply(save_row, axis=1)
                            save_df(ce_df)
                        if 'PE' in item:
                            pe_df = pd.json_normalize(item['PE'])
                            pe_df['dateTime'] = timeStamp
                            # pe_df.apply(save_row, axis=1)
                            save_df(pe_df)
                        stop1 = timeit.default_timer()
                        average_write_time_per_row += (stop1 - start1)

                stop = timeit.default_timer()
                execution_time = stop - start
                average_write_time_per_row = average_write_time_per_row / len(resJson['records']['data'])

                # diagnose problem
                if execution_time > 10:
                    json_filename = "json-{}.json".format(timeStamp.strftime('%d-%m-%Y-%H:%M:%S'))
                    with open(json_filename, 'w') as jsonfile:
                        json.dump(resJson, jsonfile)

                self.logger.debug('Option chain write time={}s, average time per row={}s'.format(execution_time,
                                                                                                 average_write_time_per_row))
        except Exception as e:
            response = self.proxyRequest.get("https://www.nseindia.com")
            self.logger.critical("Exception: " + str(e))

    def schedule_query(self, duration, instrument_type, url):
        random_duration = random.randint(duration, duration + 10)
        self.logger.debug("delay = {}".format(random_duration))
        if instrument_type == 'fut':
            self.query_fut_data(url)
        elif instrument_type == 'opt':
            self.query_opt_data(url)
        threading.Timer(random_duration, self.schedule_query, [duration, instrument_type, url]).start()


nse_website_mdc = NseWebsiteMdc()
nse_website_mdc.start()
