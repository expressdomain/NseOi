import datetime
import time
import timeit
from random import random
import pandas as pd
from random import randrange
from upstox_api.utils import Instrument

symbols = ['nifty2010019550ce', 'nifty2010019550pe', 'nifty2010019400ce', 'nifty2010019400pe', 'nifty2010019450ce',
           'nifty2010019450pe', 'nifty2010019500ce', 'nifty2010019500pe', 'nifty20100113100ce', 'nifty20100113100pe',
           'nifty20100113150ce', 'nifty20100113150pe', 'nifty20100112850ce', 'nifty20100112850pe', 'nifty20100112900ce',
           'nifty20100112900pe', 'nifty2010019650ce', 'nifty2010019650pe', 'nifty20100112750ce', 'nifty20100112750pe',
           'nifty20100112800ce', 'nifty20100112800pe', 'nifty20100112950ce', 'nifty20100112950pe', 'nifty2010019700ce',
           'nifty2010019700pe', 'nifty2010019750ce', 'nifty2010019750pe', 'nifty2010019800ce', 'nifty2010019800pe',
           'nifty2010019850ce', 'nifty2010019850pe', 'nifty2010019900ce', 'nifty2010019900pe', 'nifty2010019950ce',
           'nifty2010019950pe', 'nifty20100110000ce', 'nifty20100110000pe', 'nifty20100110050ce', 'nifty20100110050pe',
           'nifty20100110100ce', 'nifty20100110100pe', 'nifty20100110150ce', 'nifty20100110150pe', 'nifty20100110200ce',
           'nifty20100110200pe', 'nifty20100110250ce', 'nifty20100110250pe', 'nifty20100110300ce', 'nifty20100110300pe',
           'nifty20100110350ce', 'nifty20100110350pe', 'nifty20100110400ce', 'nifty20100110400pe', 'nifty20100110450ce',
           'nifty20100110450pe', 'nifty20100110500ce', 'nifty20100110500pe', 'nifty20100110550ce', 'nifty20100110550pe',
           'nifty20100110600ce', 'nifty20100110600pe', 'nifty20100110650ce', 'nifty20100110650pe', 'nifty20100110700ce',
           'nifty20100110700pe', 'nifty20100110750ce', 'nifty20100110750pe', 'nifty20100110800ce', 'nifty20100110800pe',
           'nifty20100110850ce', 'nifty20100110850pe', 'nifty20100110900ce', 'nifty20100110900pe', 'nifty20100110950ce',
           'nifty20100110950pe', 'nifty20100111000ce', 'nifty20100111000pe', 'nifty20100111050ce', 'nifty20100111050pe',
           'nifty20100111100ce', 'nifty20100111100pe', 'nifty20100111150ce', 'nifty20100111150pe', 'nifty20100111200ce',
           'nifty20100111200pe', 'nifty20100111250ce', 'nifty20100111250pe', 'nifty20100111300ce', 'nifty20100111300pe',
           'nifty20100111350ce', 'nifty20100111350pe', 'nifty20100111400ce', 'nifty20100111400pe', 'nifty20100111450ce',
           'nifty20100111450pe', 'nifty20100111500ce', 'nifty20100111500pe', 'nifty20100111550ce', 'nifty20100111550pe',
           'nifty20100113000ce', 'nifty20100113000pe', 'nifty20100113050ce', 'nifty20100113050pe', 'nifty20100111600ce',
           'nifty20100111600pe', 'nifty20100111650ce', 'nifty20100111650pe', 'nifty20100111700ce', 'nifty20100111700pe',
           'nifty20100111750ce', 'nifty20100111750pe', 'nifty20100111800ce', 'nifty20100111800pe', 'nifty20100111850ce',
           'nifty20100111850pe', 'nifty20100111900ce', 'nifty20100111900pe', 'nifty20100111950ce', 'nifty20100111950pe',
           'nifty20100112000ce', 'nifty20100112000pe', 'nifty20100112050ce', 'nifty20100112050pe', 'nifty20100112100ce',
           'nifty20100112100pe', 'nifty20100112150ce', 'nifty20100112150pe', 'nifty20100112200ce', 'nifty20100112200pe',
           'nifty20100112250ce', 'nifty20100112250pe', 'nifty20100112300ce', 'nifty20100112300pe', 'nifty20100112350ce',
           'nifty20100112350pe', 'nifty20100112400ce', 'nifty20100112400pe', 'nifty20100112450ce', 'nifty20100112450pe',
           'nifty20100112500ce', 'nifty20100112500pe', 'nifty20100112550ce', 'nifty20100112550pe', 'nifty20100112600ce',
           'nifty20100112600pe', 'nifty20100112650ce', 'nifty20100112650pe', 'nifty20100112700ce', 'nifty20100112700pe',
           'nifty2010019600ce', 'nifty2010019600pe']

message = {'timestamp': '1601006154000', 'exchange': 'NSE_FO', 'symbol': '', 'ltp': 126.05, 'close': 105.9,
           'open': 139.0, 'high': 139.0, 'low': 113.95, 'vtt': 2162775.0, 'atp': 122.91, 'oi': 1919025.0,
           'spot_price': 10919.0, 'total_buy_qty': 882150, 'total_sell_qty': 201225, 'lower_circuit': 0.05,
           'upper_circuit': 508.35, 'yearly_low': None, 'yearly_high': None, 'ltt': 1601006154000,
           'bids': [{'quantity': 75, 'price': 125.65, 'orders': 1}, {'quantity': 225, 'price': 125.55, 'orders': 1},
                    {'quantity': 150, 'price': 125.5, 'orders': 1}, {'quantity': 150, 'price': 125.45, 'orders': 2},
                    {'quantity': 450, 'price': 125.4, 'orders': 3}],
           'asks': [{'quantity': 75, 'price': 126.0, 'orders': 1}, {'quantity': 825, 'price': 126.05, 'orders': 2},
                    {'quantity': 300, 'price': 126.1, 'orders': 1}, {'quantity': 450, 'price': 126.15, 'orders': 3},
                    {'quantity': 900, 'price': 126.2, 'orders': 3}],
           'instrument': Instrument(exchange='NSE_FO', token=46827, parent_token=26000, symbol='nifty20100110900ce',
                                    name='', closing_price=105.9, expiry='1601490600000', strike_price=10900.0,
                                    tick_size=5.0, lot_size=75, instrument_type='OPTIDX', isin=None)}


def quote_message(message):
    print(message)


def generate(callback):
    symbolLen = len(symbols)
    while True:
        message['timestamp'] = int(datetime.datetime.now().strftime("%s")) * 1000
        for idx in range(0, symbolLen):
            # randomIdx = randrange(0, symbolLen)
            message['symbol'] = symbols[5]
            callback(message)
        time.sleep(1)


def generate_single_message(callback):
    message['symbol'] = symbols[5]
    callback(message)

# start_simulator(quote_message)
