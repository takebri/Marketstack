#  A program that requests stock data from Marketstack REST API and saves to csv

import requests
import csv
from configure import Marketstack_API_KEY

# Marketstack API key
api_key = Marketstack_API_KEY
# The stock symbol
symbol = 'ORCL'
# The Marketstack endpoint for end-of-day stock data
url = f'http://api.marketstack.com/v1/eod?access_key={api_key}&symbols={symbol}'

# Make the API request
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
