# Script requests stock data from Marketstack REST API and uploads to BigQuery
import requests
import logging
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from configure import api_key, project_id, dataset_name, table_name

# Logging setup
logging.basicConfig(level=logging.INFO)


def fetch_stock_data(api_key, symbol):
    """Fetches stock data from Marketstack REST API"""
    url = 'http://api.marketstack.com/v1/eod'
    params = {'access_key': api_key, 'symbols': symbol}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()    # Raise exception for HTTP error
        if response.ok:
            return response.json()['data']
        else:
            logging.error((
                f"Failed to retrieve data. HTTP Status Code: "
                f"{response.status_code}, Reason: {response.text}"
            ))
            return None
    except requests.RequestException as e:
        logging.error(f"Error during API request: {e}")
        return None


def transform_stock_data(stock_data):
    """Transforms raw stock data into format for BigQuery insertion."""
    transformed_rows = []
    for row in stock_data:
        try:
            # Check if all required keys are present in the row
            if all(key in row for key in
                    ['date', 'open', 'high', 'close', 'volume']):
                transformed_row = {
                    'Date': datetime.fromisoformat(row['date']).date()
                                    .strftime('%Y-%m-%d'),
                    'Open': row['open'],
                    'High': row['high'],
                    'Close': row['close'],
                    'Volume': row['volume']
                }
                transformed_rows.append(transformed_row)
        except KeyError as ke:
            logging.error(f"Missing key in row: {ke}. Skipping row: {row}")
        except ValueError as ve:
            logging.error(f"Invalid value in row: {ve}. Skipping row: {row}")
    return transformed_rows


def insert_into_bigquery(client, table_id, rows_to_insert):
    """Inserts transformed stock data into BigQuery."""
    try:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if not errors:
            logging.info(f"Added {len(rows_to_insert)} rows into {table_id}")
        else:
            logging.error(f"Error during BigQuery insertion: {errors}")
    except GoogleAPIError as e:
        logging.error(f"Error inserting data into BigQuery: {e}")
    return


def main():
    try:
        client = bigquery.Client()
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        stock_data = fetch_stock_data(api_key, 'ORCL')
        if stock_data:
            transformed_data = transform_stock_data(stock_data)
            insert_into_bigquery(client, table_id, transformed_data)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    return


if __name__ == "__main__":
    main()
