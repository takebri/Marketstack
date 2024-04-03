# A program that requests stock data from Marketstack REST API and uploads to BigQuery
import requests
from google.cloud import bigquery
from configure import marketstack_api_key, project_id, dataset_name, table_name


# Initialize BigQuery client
client = bigquery.Client()

# Google Cloud project ID, dataset, table name
table_id = f"{project_id}.{dataset_name}.{table_name}"

# Marketstack API key
api_key = marketstack_api_key
# Stock symbol
symbol = 'ORCL'
# marketstack api endpoint for end-of-day stock data
url = f'http://api.marketstack.com/v1/eod?access_key={api_key}&smybols={symbol}'

