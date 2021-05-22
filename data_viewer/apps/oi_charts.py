import datetime
import os
from datetime import date
import sqlalchemy

import pandas as pd

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go

from app import App
from data_viewer import oi
from common import utils
import configparser


class OiCharts:
    def __init__(self, config: configparser.ConfigParser, app: dash.Dash):
        self.app = app
        self.config = config

        self.db_type = self.config['App']['DbType']
        self.db_path = self.config.get('App', 'DbPath', vars=os.environ)

        if self.db_type == 'mysql':
            self.engine = sqlalchemy.create_engine(self.db_path, echo=False)
        elif self.db_type == 'sqllite':
            self.engine = sqlalchemy.create_engine(self.db_path, echo=False, poolclass=sqlalchemy.pool.QueuePool,
                                                   connect_args={"check_same_thread": False})
        else:
            raise str("No db.type specified")

        self.timeframe_list = utils.get_timeframe_list()
        self.expiry_list = utils.get_expiry_list()

        self.symbol_list = utils.get_symbol_list()
        self.selected_symbol = self.symbol_list[0]
        self.strike_list = utils.get_strike_list(self.selected_symbol)
        self.default_strike = {'NIFTY': 13800, 'BANKNIFTY': 30500}

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
                        dbc.Col(
                            children=[
                                dbc.Row(
                                    children=[
                                        dbc.Col(dcc.Dropdown(
                                            id='symbol',
                                            options=[
                                                {'label': item, 'value': item} for item in self.symbol_list
                                            ],
                                            value=self.symbol_list[1],
                                        ), width=2),
                                        dbc.Col(dcc.Dropdown(
                                            id='expiry',
                                            options=[
                                                {'label': item, 'value': item} for item in self.expiry_list
                                            ],
                                            value=self.expiry_list[1],
                                        ), width=2),
                                        dbc.Col(dcc.DatePickerRange(
                                            id='date-picker-range',
                                            min_date_allowed=date(2020, 10, 1),
                                            max_date_allowed=date(2021, 12, 31),
                                            # initial_visible_month=date(2017, 8, 5),
                                            start_date=date(2020, 11, 2),
                                            # end_date=date(2020, 10, 22),
                                            end_date=datetime.datetime.now().date(),
                                            display_format='DD MMM YY',
                                        ), width=4),
                                        dbc.Col(dcc.Dropdown(
                                            id='oi_buildup_strike',
                                            options=[
                                                {'label': item, 'value': item} for item in self.strike_list
                                            ],
                                            value=self.default_strike[self.selected_symbol],
                                        ), width=2),
                                        dbc.Col(html.Button(id='submit-button-state', n_clicks=0, children='Submit'),
                                                width=1),
                                    ]
                                ),
                                dcc.Graph(id='graph-open-interest')
                            ]
                            , width=7),
                        dbc.Col(
                            children=[
                                html.Iframe(
                                    height='480',
                                    width='100%',
                                    sandbox='allow-same-origin allow-scripts allow-popups allow-forms',
                                    src="https://ssltvc.forexprostools.com/?pair_ID=8985&height=480&width=800&interval=60&plotStyle=candles&domain_ID=1&lang_ID=1&timezone_ID=20"
                                ),
                                html.Iframe(
                                    height='480',
                                    width='100%',
                                    sandbox='allow-same-origin allow-scripts allow-popups allow-forms',
                                    src="https://ssltvc.forexprostools.com/?pair_ID=17950&height=480&width=800&interval=60&plotStyle=candles&domain_ID=1&lang_ID=1&timezone_ID=20"
                                )
                            ], width=5)]),

            ], )

    def register_callbacks(self, app: dash.Dash):
        @app.callback(
            [Output('oi_buildup_strike', 'options'),
             Output('oi_buildup_strike', 'value'), ],
            [Input('symbol', 'value')])
        def update_strike_dropdown(symbol):
            print('update_strike_dropdown')
            global selected_symbol, strike_list
            selected_symbol = symbol
            strike_list = common.get_strike_list(selected_symbol)

            options = [{'label': opt, 'value': opt} for opt in strike_list]
            return [options, self.default_strike[selected_symbol]]

        @app.callback(Output('graph-open-interest', "figure"),
                      [Input('submit-button-state', 'n_clicks')],
                      [State('symbol', 'value'),
                       State('expiry', 'value'),
                       State('date-picker-range', 'start_date'),
                       State('date-picker-range', 'end_date'),
                       State('oi_buildup_strike', 'value')])
        def update_oi_chart(n_clicks, symbol, expiry, start_date, end_date, strike):
            print('update_oi_chart')
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d') + datetime.timedelta(days=1)
            line_width = 1.5
            line_opacity = 0.8
            try:
                ce_df = self.get_oi_data(symbol, expiry, strike, start_date, end_date, 'CE')
                pe_df = self.get_oi_data(symbol, expiry, strike, start_date, end_date, 'PE')

                # ce_df = ce_df[ce_df['impliedVolatility'] != 0]
                # pe_df = pe_df[pe_df['impliedVolatility'] != 0]

                ce_df['dateTime'] = ce_df['dateTime'].dt.strftime('%d-%b %H:%M')

                ce_trace = go.Scatter(x=ce_df['dateTime'], y=ce_df['openInterest'], hovertext='',
                                      name='{}{}'.format(strike, 'CE'),
                                      mode='lines',
                                      line={'width': line_width},
                                      opacity=line_opacity
                                      )
                pe_trace = go.Scatter(x=ce_df['dateTime'], y=pe_df['openInterest'], hovertext='',
                                      name='{}{}'.format(strike, 'PE'),
                                      mode='lines',
                                      line={'width': line_width},
                                      opacity=line_opacity)

                underlying_trace = go.Scatter(x=ce_df['dateTime'], y=ce_df['underlyingValue'], name=symbol, yaxis="y2",
                                              mode='lines',
                                              line={'width': line_width},
                                              opacity=line_opacity)

                ce_iv_trace = go.Scatter(x=ce_df['dateTime'], y=ce_df['impliedVolatility'], hovertext='',
                                         yaxis="y3",
                                         name='{}-IV'.format('CE'),
                                         mode='lines',
                                         line={'width': line_width},
                                         opacity=line_opacity,
                                         )

                pe_iv_trace = go.Scatter(x=ce_df['dateTime'], y=pe_df['impliedVolatility'], hovertext='',
                                         yaxis="y3",
                                         name='{}-IV'.format('PE'),
                                         mode='lines',
                                         line={'width': line_width},
                                         opacity=line_opacity,
                                         )

                traces = [ce_trace, pe_trace, underlying_trace, ce_iv_trace, pe_iv_trace]

                layout = go.Layout(title='',
                                   # margin={"l": 0, "r": 0},
                                   legend={"x": 0, "y": 1.1, 'orientation': "h"},
                                   yaxis={'title': 'OI', 'tickformat': ',d', 'domain': [0.40, 1]},
                                   yaxis2={'title': 'Underlyig', 'overlaying': 'y', 'side': 'right',
                                           "showgrid": False, 'tickformat': '.2f', 'domain': [0.40, 1]},
                                   yaxis3={'title': 'IV',
                                           "showgrid": True, 'tickformat': '.2f', 'domain': [0, 0.35]},
                                   xaxis={"title": "Date",
                                          'anchor': "y3",
                                          # 'type': 'category',
                                          # 'rangebreak': {'enabled': True}
                                          'tickmode': 'linear',
                                          'tick0': 0,
                                          'dtick': len(ce_df) / 15,
                                          # 'rangeslider': {'visible': True},
                                          },
                                   height=800,
                                   hovermode='x',
                                   )

                oi_figure = go.Figure(data=traces, layout=layout)

                # oi_figure.update_xaxes(
                #     dtick="M1",
                #     tickformat="%b\n%Y",
                #     ticklabelmode="period")

                # oi_figure.update_xaxes(
                #     rangebreak=[
                #         dict(bounds=[16, 9], pattern="hour"),  # hide hours outside of 9am-4pm
                #         dict(bounds=["sat", "mon"]),
                #     ]
                # )

                return oi_figure
            except Exception as e:
                print(e)
                raise dash.exceptions.PreventUpdate

    def get_oi_data(self, symbol, expiry, strike, start_date, end_date, type):
        table_name = 'OPTIDX{}{}{}{}'.format(symbol, expiry, type, strike)
        df = pd.read_sql("SELECT * FROM '{}'".format(table_name), con=self.engine)
        df.drop_duplicates(keep='last', inplace=True, subset=['dateTime'])
        df['dateTime'] = pd.to_datetime(df['dateTime'], errors='coerce')
        df['impliedVolatility'].replace(to_replace=0, method='ffill', inplace=True)
        df['underlyingValue'].replace(to_replace=0, method='ffill', inplace=True)
        df = df[(df.dateTime >= start_date) & (df.dateTime <= end_date)]
        return df
