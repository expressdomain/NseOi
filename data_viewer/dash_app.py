import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from app import App
from data_viewer.apps.oi_buildup import OiBuildup
from data_viewer.apps.oi_charts import OiCharts
from data_viewer.apps.option_chain import OptionChain
from data_viewer.apps.option_premium import OptionPremium


class DashApp(App):

    def __init__(self):
        super().__init__()

        # Dash App
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
        self.app.layout = html.Div([
            dcc.Location(id='url', refresh=False),
            html.Div(id='page-content')
        ])


    def start(self):
        self.register_apps(self.app)
        self.app.run_server(host=self.config.get('Dash', 'host'), port=self.config.getint('Dash', 'port'),
                            debug=self.config.getboolean('Dash', 'Debug'))


    def register_apps(self, app: dash.Dash):
        oi_buildup = OiBuildup(self.config, app)
        # oi_charts = OiCharts(self.config, app)
        option_chain = OptionChain(self.config, app)
        option_premium = OptionPremium(self.config, app)

        @app.callback(Output('page-content', 'children'),
                      [Input('url', 'pathname')])
        def display_page(pathname):
            if pathname == '/':
                return option_chain.get_layout()
            # elif pathname == '/oi-chart':
            #     return oi_charts.get_layout()
            elif pathname == '/oi-buildup':
                return oi_buildup.get_layout()
            elif pathname == '/premium':
                return option_premium.get_layout()
            else:
                return '404'


if __name__ == '__main__':
    dash_app = DashApp()
    # option_oi_buildup.build_layout()
    dash_app.start()
