import configparser
import os
import time
import timeit
from datetime import date
import datetime

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import matplotlib
import pandas as pd
import sqlalchemy
from dash.dependencies import Input, Output, State, ALL, MATCH
from sqlalchemy.pool import StaticPool

from common import utils, datetime_utils


# from data_viewer import common


class OptionChain:
    def __init__(self, config: configparser.ConfigParser, app: dash.Dash):
        self.app = app
        self.config = config

        self.db_type = self.config['App']['DbType']
        self.db_path = self.config.get('App', 'DbPath', vars=os.environ)

        if self.db_type == 'mysql':
            self.engine = sqlalchemy.create_engine(self.db_path, echo=False)
        elif self.db_type == 'sqllite':
            self.engine = sqlalchemy.create_engine(self.db_path, echo=False, poolclass=StaticPool,
                                                   connect_args={"check_same_thread": False})
        else:
            raise str("No db.type specified")

        self.timeframe_list = utils.get_timeframe_list()
        self.expiry_list = utils.get_expiry_list()
        self.historical_expires, self.future_expires = utils.get_historical_future_expires()

        self.symbol_list = utils.get_symbol_list()
        self.selected_symbol = self.symbol_list[1]
        self.strike_list = utils.get_strike_list(self.selected_symbol)
        self.default_strike = {'NIFTY': 14300, 'BANKNIFTY': 32000}

        self.layout = None
        self.register_callbacks(app)

    def get_layout(self):
        return html.Div(
            style={
                'padding': 20
            },
            children=[
                dbc.Row(
                    children=[
                        dbc.Col(dcc.Dropdown(
                            id='symbol',
                            options=[
                                {'label': item, 'value': item} for item in self.symbol_list
                            ],
                            value=self.symbol_list[0],
                        ), width=2),
                        dbc.Col(dcc.Dropdown(
                            id='expiry',
                            options=
                            [
                                {'label': item, 'value': item} for item in self.future_expires[:4]
                            ] +
                            [{'label': '---------------------', 'value': '-'}] +
                            [
                                {'label': item, 'value': item} for item in self.historical_expires
                            ],
                            value=self.future_expires[0],
                        ), width=2),

                        dbc.Col(dbc.Button(id='submit-button-state', color="success", n_clicks=0,
                                           children='Get'),
                                width=1),

                        dbc.Col(dcc.DatePickerRange(
                            id='date-picker-range',
                            min_date_allowed=date(2020, 10, 1),
                            max_date_allowed=date(2021, 12, 31),
                            # initial_visible_month=date(2017, 8, 5),
                            # start_date=date(2020, 11, 5),
                            # end_date=date(2020, 11, 6),
                            start_date=datetime.datetime.now().date() - datetime.timedelta(days=2),
                            end_date=datetime.datetime.now().date(),
                            display_format='DD MMM YY',
                        ), width=3),

                        dbc.Col(children=[html.A('center', href="#table-center-row"),
                                          html.Div(id='last-update-time', children=[])],
                                width=3),
                        dcc.Interval(
                            id='interval-component',
                            interval=60 * 1000 * 4,  # in milliseconds
                            n_intervals=0
                        )
                    ]),
                dbc.Row(
                    children=[
                        dbc.Col(
                            children=[

                                # dcc.Textarea(id='textarea-example-output', value='initial value'),
                                dbc.Row(id='option-chain-table',
                                        children=[],
                                        style={
                                            'margin-top': 10,
                                            'padding': 10,
                                            'height': 850,
                                            'overflowY': 'scroll',
                                            'overflowX': 'hidden'
                                        }
                                        )

                            ]
                            , width=6),
                        dbc.Col(
                            children=[
                                dcc.Graph(id='option-chain-chart')
                            ], width=6)
                    ]),

            ], )

    def register_callbacks(self, app: dash.Dash):
        def gradient_color(df, column):
            import seaborn as sns
            from matplotlib.colors import Normalize
            import matplotlib.pyplot as plt
            from matplotlib import colors

            # cmap = sns.color_palette('vlag', as_cmap=True)
            cmap = sns.diverging_palette(20, 220, s=60, l=50, as_cmap=True)

            # Normalize data
            norm = Normalize(vmin=df[column].min(), vmax=df[column].max())
            c = cmap(norm(df[column]))
            c = [matplotlib.colors.rgb2hex(color) for color in c]

            # cmap = 'PuBu'
            # m = df[column].min().min()
            # M = df[column].max()
            # rng = M - m
            # norm = colors.Normalize(m - (rng * 0),
            #                         M + (rng * 0))
            # normed = norm(df[column].values)
            # c = [colors.rgb2hex(x) for x in plt.cm.get_cmap(cmap)(normed)]

            return c

        def get_selected_strikes(check_list_values):
            selected_strikes = []
            for item in check_list_values:
                if len(item) > 0:
                    selected_strikes.append(item[0])
            return selected_strikes

        @app.callback(
            Output('option-chain-chart', "figure"),
            [Input({'type': 'strike', 'index': ALL}, 'value'),
             Input('date-picker-range', 'start_date'),
             Input('date-picker-range', 'end_date'),
             ],
            [State('symbol', 'value'),
             State('expiry', 'value'),
             ]
        )
        def display_output(check_list_values, start_date, end_date, symbol, expiry, ):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d') + datetime.timedelta(days=1)
            selected_strikes = get_selected_strikes(check_list_values)

            # if len(option_list) > 0:
            return self.get_oi_chart(symbol, expiry, start_date, end_date, selected_strikes)
            # build_oi_chart()
            # ctx = dash.callback_context
            #
            # if ctx.triggered:
            #     button_id = ctx.triggered[0]['prop_id'].split('.')[0]
            #     return [str(button_id)]
            #     # print(button_id)
            # return ['1']
            # return None

        @app.callback(
            [Output('option-chain-table', 'children'),
             Output('last-update-time', 'children')],
            [Input('submit-button-state', 'n_clicks'),
             Input('interval-component', 'n_intervals')],
            [State('symbol', 'value'),
             State('expiry', 'value'),
             State({'type': 'strike', 'index': ALL}, 'value')])
        def update_option_chain(n_clicks, n_intervals, symbol, expiry, check_list_values):
            self.app.title = '{} - Chain'.format(symbol)
            # print('Refreshing: '.format(n_intervals))
            self.selected_symbol = symbol
            self.strike_list = utils.get_strike_list(symbol)

            # option_oi_data = oi.get_option_oi_buildup_range(symbol, expiry, strike, timeframe, 100)
            start = timeit.default_timer()
            df = self.get_option_chain(symbol, expiry)
            # df.to_csv('/mnt/hgfs/win-d/work/stock_market/data_viewer/apps/test_data/option_chain.csv')
            # df = pd.read_csv('/mnt/hgfs/win-d/work/stock_market/data_viewer/apps/test_data/option_chain.csv')
            stop = timeit.default_timer()
            execution_time = stop - start
            print("Time taken to build option chain: {}".format(execution_time))

            # return df.to_dict('records'), style_data_conditional
            option_chain = df[['CE_OI', 'CE_OI_CHG', 'CE_LP',
                               'strike',
                               'PE_LP', 'PE_OI_CHG', 'PE_OI']]

            # option_chain = df[['CE_OI', 'CE_OI_CHG', 'CE_IV', 'CE_vol', 'CE_LP',
            #                    'strike',
            #                    'PE_LP', 'PE_vol', 'PE_IV', 'PE_OI_CHG', 'PE_OI']]
            table = dbc.Table.from_dataframe(option_chain, striped=True, bordered=True, hover=True)

            def color_scale(df, column_name, table, col_idx):
                # table.children[1].children[0].children[0].style = {'background-color': '#bde9ba'}
                scale = gradient_color(df, column_name)
                for idx, color in enumerate(scale):
                    table.children[1].children[idx].children[col_idx].style = {'background-color': color}

            color_scale(option_chain, 'CE_OI_CHG', table, 1)
            color_scale(option_chain, 'PE_OI_CHG', table, 5)

            # color_scale(option_chain, 'PE_OI_CHG', table, 1)

            def set_column_bars(df, table, column_idx, left_to_right=True):
                normalized_df = (df - df.min()) / (
                        df.max() - df.min())
                for idx, size in enumerate(normalized_df):
                    table.children[1].children[idx].children[column_idx].style = {
                        'width': 200,
                        'background-image': 'linear-gradient(#0091ea,#0091ea)',
                        'background-repeat': 'no-repeat',
                        'background-size': '{}%'.format(size * 100),
                    }
                    if left_to_right is False:
                        table.children[1].children[idx].children[column_idx].style['background-position'] = '100% 100%'

            set_column_bars(option_chain['CE_OI'], table, 0)
            set_column_bars(option_chain['PE_OI'], table, 6, False)

            # add checkbox
            selected_strikes = get_selected_strikes(check_list_values)
            table.children[0].children.children.insert(0, html.Th(style={'width': 40}))
            table.children[0].children.children.append(html.Th(style={'width': 40}))
            for idx, row in enumerate(table.children[1].children):
                if idx == (int(len(table.children[1].children) / 2) - 16):
                    row.id = 'table-center-row'
                ce_option_value = "{}-{}".format(option_chain.iloc[idx]['strike'].astype(int), 'CE')
                ce_value = [ce_option_value] if ce_option_value in selected_strikes else []

                pe_option_value = "{}-{}".format(option_chain.iloc[idx]['strike'].astype(int), 'PE')
                pe_value = [pe_option_value] if pe_option_value in selected_strikes else []

                row.children.insert(0, html.Td(
                    dcc.Checklist(
                        id={'type': 'strike', 'index': idx},
                        options=[{"label": "",
                                  "value": ce_option_value}],
                        value=ce_value,
                    )
                ))
                row.children.append(html.Td(
                    dcc.Checklist(
                        id={'type': 'strike', 'index': idx},
                        options=[{"label": "",
                                  "value": pe_option_value}],
                        value=pe_value,
                    )
                ))

            # option chain divider color
            for row_idx in range(len(df)):
                for col_idx in range(len(table.children[0].children.children)):
                    if col_idx in [0, 1, 3, 4, 5] and df.iloc[row_idx]['CE_underlyingValue'] > df.iloc[row_idx][
                        'strike']:
                        if hasattr(table.children[1].children[row_idx].children[col_idx], 'style'):
                            table.children[1].children[row_idx].children[col_idx].style['background-color'] = '#ffecb3'
                        else:
                            table.children[1].children[row_idx].children[col_idx].style = {
                                'background-color': '#ffecb3'}
                    elif col_idx in [7, 8, 9, 11, 12] and df.iloc[row_idx]['CE_underlyingValue'] < df.iloc[row_idx][
                        'strike']:
                        if hasattr(table.children[1].children[row_idx].children[col_idx], 'style'):
                            table.children[1].children[row_idx].children[col_idx].style['background-color'] = '#ffecb3'
                        else:
                            table.children[1].children[row_idx].children[col_idx].style = {
                                'background-color': '#ffecb3'}
            table_row = [table]
            return table_row, 'Last update time: {}'.format(time.strftime("%I:%M:%S %p"))

    def get_option_chain(self, symbol, expiry):
        expiry = datetime_utils.convert_date(expiry, '%d-%b-%Y', '%d-%m-%Y')

        def get_row(symbol, expiry, option_type, strike):
            table_name = 'OPTIDX{}{}{}{}'.format(symbol, expiry, option_type, strike)
            df = pd.read_sql("SELECT * FROM '{}' ORDER BY rowid DESC LIMIT 1;".format(table_name), con=self.engine)
            df.rename({"openInterest": "OI", 'lastPrice': 'LP', 'totalTradedVolume': 'vol', 'impliedVolatility': 'IV',
                       'changeinOpenInterest': 'OI_CHG'},
                      axis='columns', inplace=True)
            df = df.add_prefix(option_type + '_')
            return df

        option_chain = pd.DataFrame()

        for strike in self.strike_list:
            try:
                ce_df = get_row(symbol, expiry, 'CE', strike)
                pe_df = get_row(symbol, expiry, 'PE', strike)
                ce_df['strike'] = strike
                row = pd.concat([ce_df, pe_df], axis=1, sort=False)
                option_chain = option_chain.append(row)
            except Exception as e:
                print(e)
                pass

        underlying_value = option_chain.iloc[0]['CE_underlyingValue']
        # option_chain = option_chain[(option_chain['strike'] >= (underlying_value - 900)) & (
        #         option_chain['strike'] <= (underlying_value + 900))]
        return option_chain

    def get_oi_chart(self, symbol, expiry, start_date, end_date, optionsList):
        expiry = datetime_utils.convert_date(expiry, '%d-%b-%Y', '%d-%m-%Y')

        def get_oi_data(symbol, expiry, start_date, end_date, strike, option_type):
            table_name = 'OPTIDX{}{}{}{}'.format(symbol, expiry, option_type, strike)
            series = pd.read_sql("SELECT * FROM '{}'".format(table_name), con=self.engine)
            series.drop_duplicates(keep='last', inplace=True, subset=['dateTime'])
            series['dateTime'] = pd.to_datetime(series['dateTime'], errors='coerce')
            series['impliedVolatility'].replace(to_replace=0, method='ffill', inplace=True)
            series['underlyingValue'].replace(to_replace=0, method='ffill', inplace=True)
            series['openInterest'].replace(to_replace=0, method='ffill', inplace=True)
            series = series[(series.dateTime >= start_date) & (series.dateTime <= end_date)]
            series['dateTime'] = series['dateTime'].dt.strftime('%d-%b %H:%M')
            return series

        def get_trace(x, y, name, width=1.1, color=None, yaxis=None):
            return go.Scatter(x=x, y=y, hovertext='', yaxis=yaxis,
                              name=name,
                              mode='lines',
                              line={'width': width, 'color': color},
                              opacity=0.8
                              )

        underlying_trace = []
        oi_traces = []
        iv_traces = []
        total_points = 0
        x_axis = None
        for idx, option in enumerate(optionsList):
            decoded_option = option.split('-')
            df = get_oi_data(symbol, expiry, start_date, end_date, decoded_option[0], decoded_option[1])
            if idx == 0:
                x_axis = df['dateTime']
                underlying_trace.append(
                    get_trace(x_axis, df['underlyingValue'], "Underlying", yaxis='y2', color='black'))
            total_points = len(df)
            oi_traces.append(get_trace(x_axis, df['openInterest'], option, yaxis=None))
            iv_traces.append(get_trace(x_axis, df['impliedVolatility'], "{}-IV".format(option), yaxis='y3'))

        layout = go.Layout(title='',
                           legend={"x": 1, "y": 0, 'orientation': "v"},
                           yaxis={'title': 'OI', 'gridcolor': "#eeeeee", 'linecolor': "#636363", 'tickformat': ',d',
                                  'domain': [0.40, 1]},
                           yaxis2={'title': 'Underlyig', 'overlaying': 'y', 'side': 'right', 'gridcolor': "#eeeeee",
                                   'linecolor': "#636363",
                                   "showgrid": False, 'tickformat': '.2f', 'domain': [0.40, 1]},
                           yaxis3={'title': 'IV', 'gridcolor': "#eeeeee", 'linecolor': "#636363",
                                   "showgrid": True, 'tickformat': '.2f', 'domain': [0, 0.35]},

                           xaxis={"title": "Date",
                                  'anchor': "y3",
                                  # 'type': 'category',
                                  # 'rangebreak': {'enabled': True}
                                  'tickmode': 'linear',
                                  'tick0': 0,
                                  'dtick': total_points / 15,
                                  'linecolor': "#636363",
                                  'gridcolor': "#eeeeee",
                                  # 'rangeslider': {'visible': True},
                                  },
                           height=800,
                           hovermode='x',
                           plot_bgcolor='white'
                           )
        oi_figure = go.Figure(data=oi_traces + underlying_trace + iv_traces, layout=layout)
        return oi_figure
