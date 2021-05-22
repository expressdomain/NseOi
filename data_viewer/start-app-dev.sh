#!/bin/bash

export PYTHONPATH=/home/rtalokar/win-d/work/stock_market
# shellcheck disable=SC2164
cd $PYTHONPATH/data_viewer
python dash_app.py DashApp-Dev.ini
