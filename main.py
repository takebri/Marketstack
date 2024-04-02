#  A program that requests stock data from Marketstack REST API and saves to csv

import requests
import csv
from configure import Marketstack_API_KEY

# Marketstack API key
api_key = Marketstack_API_KEY
# The stock symbol
symbol = 'ORCL'
# The Marketstack endpoint for end-of-day stock data
url = f'http://api.marketstack.com/v1/eod?access_key={api_key}& symbols={symbol}'
# CSV output
output_csv = 'stock_data.csv'


# Make the API request
response = requests.get(url)

if response.status_code == 200:
    stock_data = response.json()

    # stock_data['data'] contains the returned data object
    with open(output_csv, 'w', newline='') as file:
        writer = csv.writer(file)
        # Write the header
        writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

        # Write the stock data
        for item in stock_data['data']:
            writer.writerow([
                item['date'],
                item['open'],
                item['high'],
                item['low'],
                item['close'],
                item['volume']]
                )
    print(f'Stock data successfully written to {output_csv}')
