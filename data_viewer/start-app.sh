#!/bin/bash

export PYTHONPATH=/home/ubuntu/projects/stock_market
# shellcheck disable=SC2164
cd $PYTHONPATH/data_viewer
python3.7 dash_app.py DashApp.ini
