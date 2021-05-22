import datetime
import os
from datetime import date
import sqlalchemy

import pandas as pd

import dash
import dash_table
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from sqlalchemy.pool import StaticPool

from app import App
from common import utils
from data_viewer import oi
import configparser

class OiBuildup:
    def __init__(self, config: configparser.ConfigParser, app: dash.Dash):
        self.app = app
        self.config = config

        self.db_type = self.config['App']['DbType']
        self.db_path = self.config.get('App', 'DbPath', vars=os.environ)
        # self.logger.info("db_type: {}, db_path={}".format(self.db_type, self.db_path))

        if self.db_type == 'mysql':
            self.engine = sqlalchemy.create_engine(self.db_path, echo=False)
        elif self.db_type == 'sqllite':
            self.engine = sqlalchemy.create_engine(self.db_path, echo=False, poolclass=StaticPool,
                                                   connect_args={"check_same_thread": False})
        else:
            raise str("No db.type specified")

        self.timeframe_list = utils.get_timeframe_list()
        self.expiry_list = utils.get_expiry_list()

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
                        dbc.Col(
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
                                            options=[
                                                {'label': item, 'value': item} for item in self.expiry_list
                                            ],
                                            value=self.expiry_list[1],
                                        ), width=2),
                                        dbc.Col(dcc.DatePickerSingle(
                                            id='buildup_date',
                                            style={"height": 30},
                                            min_date_allowed=date(2020, 10, 1),
                                            max_date_allowed=date(2022, 10, 1),
                                            date=datetime.datetime.now().date(),
                                            # date=date(2020, 10, 9)
                                        ), width=2),
                                        dbc.Col(dcc.Dropdown(
                                            id='strike',
                                            options=[
                                                {'label': item, 'value': item} for item in self.strike_list
                                            ],
                                            value=self.default_strike[self.selected_symbol],
                                        ), width=2),
                                        dbc.Col(dcc.Dropdown(
                                            id='timeframe',
                                            options=[
                                                {'label': item, 'value': item} for item in self.timeframe_list
                                            ],
                                            value=15,
                                        ), width=2),
                                        dbc.Col(html.Button(id='submit-button-state', n_clicks=0, children='Submit'),
                                                width=1),
                                    ]
                                ),
                                html.Br(),
                                dash_table.DataTable(
                                    id='datatable-oi-buildup',
                                    columns=[
                                        {"name": i, "id": i} for i in
                                        # ['DateTime',
                                        #  'CE_LP', 'CE_vol', 'CE_IV', 'CE_OI', 'CE_OI_CHG', 'CE_OI_INTERPRETATION',
                                        #  'LP', 'LP_CHG',
                                        #  'PE_OI_INTERPRETATION', 'PE_OI_CHG', 'PE_OI', 'PE_IV', 'PE_vol', 'PE_LP']
                                        ['DateTime', 'ExactDateTime',
                                         'CE_LP', 'CE_OI', 'CE_OI_CHG', 'CE_OI_ANALYSIS',
                                         'LP', 'LP_CHG',
                                         'PE_OI_ANALYSIS', 'PE_OI_CHG', 'PE_OI', 'PE_LP']
                                    ],
                                    style_cell={'textAlign': 'center'},
                                    # row_selectable='single',
                                    style_table={
                                        'padding': 20,
                                        'height': 850,
                                        'overflowY': 'scroll',
                                        'overflowX': 'hidden'
                                    }
                                )
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
            [Output('strike', 'options'),
             Output('strike', 'value'), ],
            [Input('symbol', 'value')]
        )
        def update_strike_dropdown(symbol):
            print('update_strike_dropdown')
            global selected_symbol, strike_list
            selected_symbol = symbol
            strike_list = utils.get_strike_list(selected_symbol)

            options = [{'label': opt, 'value': opt} for opt in strike_list]
            return [options, self.default_strike[selected_symbol]]

        @app.callback([Output('datatable-oi-buildup', 'data'),
                       Output('datatable-oi-buildup', 'style_data_conditional')],
                      [Input('submit-button-state', 'n_clicks')],
                      [State('symbol', 'value'),
                       State('expiry', 'value'),
                       State('buildup_date', 'date'),
                       State('strike', 'value'),
                       State('timeframe', 'value'), ])
        def update_oi_buildup(n_clicks, symbol, expiry, buildup_date, strike, timeframe):
            print('update_oi_buildup')
            # option_oi_data = oi.get_option_oi_buildup_range(symbol, expiry, strike, timeframe, 100)
            option_oi_data = oi.get_option_oi_buildup(self.engine, symbol, expiry, buildup_date, strike, timeframe)
            option_oi_data['DateTime'] = option_oi_data['DateTime'].dt.time

            def calculate_time_range(df):
                start = datetime.datetime.combine(date.today(), df)
                end = start + datetime.timedelta(minutes=timeframe)
                return start.strftime('%H:%M') + " - " + end.strftime('%H:%M')

            option_oi_data['DateTime'] = option_oi_data['DateTime'].apply(calculate_time_range)
            option_oi_data['ExactDateTime'] = option_oi_data['CE_dateTime'].dt.time
            # backTestData['ExitDate'] = backTestData['ExitDate'].dt.strftime('%d-%b-%Y')
            option_oi_data = option_oi_data[::-1]

            (styles, legend) = discrete_background_color_bins(option_oi_data, columns=['LP'])

            style_data_conditional = oi_buildup_style(oi.SHORT_BUILDUP, '#ba6b6c', 'white') + \
                                     oi_buildup_style(oi.LONG_BUILDUP, '#75a478', 'white') + \
                                     oi_buildup_style(oi.SHORT_COVERING, '#26418f', 'white') + \
                                     oi_buildup_style(oi.LONG_UNWINDING, '#fdd835', 'black') + \
                                     data_bars(option_oi_data, 'CE_OI') + \
                                     data_bars(option_oi_data, 'PE_OI') + \
                                     styles

            return option_oi_data.to_dict('records'), style_data_conditional

        def discrete_background_color_bins(df, n_bins=5, columns='all'):
            import colorlover
            bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]
            if columns == 'all':
                if 'id' in df:
                    df_numeric_columns = df.select_dtypes('number').drop(['id'], axis=1)
                else:
                    df_numeric_columns = df.select_dtypes('number')
            else:
                df_numeric_columns = df[columns]
            df_max = df_numeric_columns.max().max()
            df_min = df_numeric_columns.min().min()
            ranges = [
                ((df_max - df_min) * i) + df_min
                for i in bounds
            ]
            styles = []
            legend = []
            for i in range(1, len(bounds)):
                min_bound = ranges[i - 1]
                max_bound = ranges[i]
                backgroundColor = colorlover.scales[str(n_bins)]['seq']['YlGn'][i - 1]
                color = 'white' if i > len(bounds) / 2. else 'inherit'

                for column in df_numeric_columns:
                    styles.append({
                        'if': {
                            'filter_query': (
                                    '{{{column}}} >= {min_bound}' +
                                    (' && {{{column}}} < {max_bound}' if (i < len(bounds) - 1) else '')
                            ).format(column=column, min_bound=min_bound, max_bound=max_bound),
                            'column_id': column
                        },
                        'backgroundColor': backgroundColor,
                        'color': color
                    })
                legend.append(
                    html.Div(style={'display': 'inline-block', 'width': '60px'}, children=[
                        html.Div(
                            style={
                                'backgroundColor': backgroundColor,
                                'borderLeft': '1px rgb(50, 50, 50) solid',
                                'height': '10px'
                            }
                        ),
                        html.Small(round(min_bound, 2), style={'paddingLeft': '2px'})
                    ])
                )

            return (styles, html.Div(legend, style={'padding': '5px 0 5px 0'}))

        def oi_buildup_style(buildup_type, bg_color, txt_color):
            return [
                {
                    'if': {
                        'column_id': cell_text,
                        'filter_query': '{%s} = "%s"' % (cell_text, buildup_type)
                    },
                    'backgroundColor': bg_color,
                    'color': txt_color,
                    'textAlign': 'center'
                } for cell_text in ['CE_OI_ANALYSIS', 'PE_OI_ANALYSIS']
            ]

        def data_bars(df, column):
            n_bins = 100
            bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]
            ranges = [
                ((df[column].max() - df[column].min()) * i) + df[column].min()
                for i in bounds
            ]
            styles = []
            for i in range(1, len(bounds)):
                min_bound = ranges[i - 1]
                max_bound = ranges[i]
                max_bound_percentage = bounds[i] * 100
                styles.append({
                    'if': {
                        'filter_query': (
                                '{{{column}}} >= {min_bound}' +
                                (' && {{{column}}} < {max_bound}' if (i < len(bounds) - 1) else '')
                        ).format(column=column, min_bound=min_bound, max_bound=max_bound),
                        'column_id': column
                    },
                    'background': (
                        """
                            linear-gradient(90deg,
                            #bbdefb 0%,
                            #bbdefb {max_bound_percentage}%,
                            white {max_bound_percentage}%,
                            white 100%)
                        """.format(max_bound_percentage=max_bound_percentage)
                    ),
                    'paddingBottom': 2,
                    'paddingTop': 2,
                    'width': '100px'
                })

            return styles

    def get_oi_data(self, symbol, expiry, strike, start_date, end_date, type):
        table_name = 'OPTIDX{}{}{}{}'.format(symbol, expiry, type, strike)
        df = pd.read_sql("SELECT * FROM '{}'".format(table_name), con=self.engine)
        df.drop_duplicates(keep='last', inplace=True, subset=['dateTime'])
        df['dateTime'] = pd.to_datetime(df['dateTime'], errors='coerce')
        df = df[(df.dateTime >= start_date) & (df.dateTime <= end_date)]
        return df

