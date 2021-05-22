import numpy as np
import pandas as pd

DEFAULT = '-'
LONG_BUILDUP = 'Long Buildup'
SHORT_BUILDUP = 'Short Buildup'
LONG_UNWINDING = 'Long Unwinding'
SHORT_COVERING = 'Short Covering'


def interpret_oi(df):
    def interpret(oi_chg, underlying_chg, side):
        if side == 'PE':
            if oi_chg > 0 and underlying_chg > 0:
                return SHORT_BUILDUP
            elif oi_chg > 0 and underlying_chg < 0:
                return LONG_BUILDUP
            elif oi_chg < 0 and underlying_chg < 0:
                return SHORT_COVERING
            elif oi_chg < 0 and underlying_chg > 0:
                return LONG_UNWINDING
        elif side == 'CE':
            if oi_chg > 0 and underlying_chg < 0:
                return SHORT_BUILDUP
            elif oi_chg > 0 and underlying_chg > 0:
                return LONG_BUILDUP
            elif oi_chg < 0 and underlying_chg > 0:
                return SHORT_COVERING
            elif oi_chg < 0 and underlying_chg < 0:
                return LONG_UNWINDING

    ce = interpret(df['CE_OI_CHG'], df['LP_CHG'], 'CE')
    pe = interpret(df['PE_OI_CHG'], df['LP_CHG'], 'PE')
    return pd.Series([ce, pe])


def get_option_data(sql_engine, symbol, expiry, buildup_date, type, strike, timeframe):
    table_name = 'OPTIDX' + symbol + expiry + type + str(strike)
    print('tablename = {}'.format(table_name))
    df = pd.read_sql("SELECT * FROM '{}'".format(table_name), con=sql_engine)
    df.drop_duplicates(keep='last', inplace=True, subset=['dateTime'])
    df['dateTime'] = pd.to_datetime(df['dateTime'], errors='coerce')
    df = df.resample('{}T'.format(timeframe), on='dateTime').last()
    df.dropna(inplace=True, subset=['openInterest'])
    df['OI_CHG'] = df['openInterest'].diff()
    df['UNDERLYING_CHG'] = df['underlyingValue'].diff()
    df = df[df.dateTime > buildup_date]
    # df = df[
    #     ['dateTime', 'openInterest', 'OI_CHG', 'lastPrice', 'totalTradedVolume', 'impliedVolatility', 'underlyingValue',
    #      'UNDERLYING_CHG']]
    df.rename({"openInterest": "OI", 'lastPrice': 'LP', 'totalTradedVolume': 'vol', 'impliedVolatility': 'IV'},
              axis='columns', inplace=True)
    df = df.fillna(0)
    convert_dict = {'OI': int, 'OI_CHG': int}
    df = df.astype(convert_dict)
    df = df.add_prefix(type + '_')
    #     df = df.round(decimals=2)
    #     df.rename({'{}_Undrlying'.format(type): "Undrlying"},axis='columns',inplace =True)
    # df.drop(df.columns[0], axis=1, inplace=True)
    return df


def get_option_oi_buildup(sql_engine, symbol, expiry, buildup_date, strike, timeframe):
    ceDf = get_option_data(sql_engine, symbol, expiry, buildup_date, 'CE', strike, timeframe)
    peDf = get_option_data(sql_engine, symbol, expiry, buildup_date, 'PE', strike, timeframe)
    result = pd.concat([ceDf, peDf], axis=1, sort=False)
    result = result.round(2)
    # result.drop(result.columns[[12, 13]], axis=1, inplace=True)
    result.rename({'CE_underlyingValue'.format(type): "LP"}, axis='columns', inplace=True)
    result.rename({'CE_UNDERLYING_CHG'.format(type): "LP_CHG"}, axis='columns', inplace=True)
    result['DateTime'] = result.index.values
    result[['CE_OI_ANALYSIS', 'PE_OI_ANALYSIS']] = result.apply(interpret_oi, axis=1)
    return result


def get_option_oi_buildup_range(sql_engine, symbol, expiry, buildup_date, strike, timeframe, range):
    option_oi_data = get_option_oi_buildup(sql_engine, symbol, expiry, buildup_date, strike, timeframe)
    for i in np.arange(strike - range, strike + range + 50, 50):
        if i != strike:
            temp = get_option_oi_buildup(symbol, expiry, i, timeframe)
            option_oi_data['CE_OI_CHG'] = option_oi_data['CE_OI_CHG'] + temp['CE_OI_CHG']
            option_oi_data['PE_OI_CHG'] = option_oi_data['PE_OI_CHG'] + temp['PE_OI_CHG']

    option_oi_data[['CE_OI_ANALYSIS', 'PE_OI_ANALYSIS']] = option_oi_data.apply(interpret_oi, axis=1)
    return option_oi_data
