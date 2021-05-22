#!/bin/bash

export PYTHONPATH=/home/ubuntu/projects/stock_market
# shellcheck disable=SC2164
cd $PYTHONPATH/market_data/nse_website
python3.7 nse_website_mdc.py NseWebsiteMdc.ini
