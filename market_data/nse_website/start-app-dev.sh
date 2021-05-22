#!/bin/bash

export PYTHONPATH=/home/rtalokar/win-d/work/stock_market
# shellcheck disable=SC2164
cd $PYTHONPATH/market_data/nse_website
python nse_website_mdc.py NseWebsiteMdc-Dev.ini
