import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from log import log_message

# Load API key from .env file
load_dotenv()

API_KEY = os.getenv("API_KEY")

# API URL
API_URL = "https://www.alphavantage.co/query"

# List of symbols to process
SYMBOLS = {
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corporation",
    "GOOG": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc.",
    "NFLX": "Netflix Inc."
}

def fetch_stock_data(symbol):
    """
    Fetch daily stock data for a specific symbol from Alpha Vantage.

    Args:
        symbol (str): The stock symbol.

    Returns:
        pd.DataFrame: DataFrame with stock data, or None on failure.
    """
    log_message("info", f"Fetching data for symbol: {symbol}")

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "datatype": "json",
        "outputsize": "compact"
    }

    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        # Check for valid data format
        if "Time Series (Daily)" not in data:
            log_message("error", f"Invalid data format for {symbol}: {data}")
            return None

        # Parse into DataFrame
        time_series = data["Time Series (Daily)"]
        df = pd.DataFrame.from_dict(time_series, orient="index").reset_index()
        
        df.rename(columns={
            "index": "Date",
            "1. open": "OpenPrice",
            "2. high": "High",
            "3. low": "Low",
            "4. close": "ClosePrice",
            "5. volume": "Volume"
        }, inplace=True)

        df["Symbol"] = symbol
        return df

    except Exception as e:
        log_message("error", f"Error fetching data for {symbol}: {e}")
        return None

def parse_stock_data(symbol, stock_data):
    """
    Parse the JSON data into a structured DataFrame.

    Args:
        symbol (str): The stock symbol.
        stock_data (dict): Raw JSON data.

    Returns:
        pd.DataFrame: Parsed DataFrame.
    """
    try:
        company_name = SYMBOLS[symbol]
        records = []
        for date, metrics in stock_data.items():
            records.append({
                "Symbol": symbol,
                "CompanyName": company_name,
                "Date": date,
                "OpenPrice": float(metrics.get("1. open", 0)),
                "High": float(metrics.get("2. high", 0)),
                "Low": float(metrics.get("3. low", 0)),
                "ClosePrice": float(metrics.get("4. close", 0)),
                "Volume": int(metrics.get("6. volume", 0))
            })

        df = pd.DataFrame(records)
        return df

    except Exception as e:
        log_message("error", f"Error parsing data for {symbol}: {e}")
        return pd.DataFrame()

def main():
    """
    Main function to fetch stock data for multiple symbols and save results.
    """
    log_message("info", "Starting data extraction process")
    
    all_data = []  # To store data for all symbols
    total_rows = 0  # Counter for total rows processed

    for symbol in SYMBOLS.keys():
        df = fetch_stock_data(symbol)

        if df is not None:
            row_count = len(df)
            log_message("info", f"Extracted {row_count} rows for {symbol}")
            total_rows += row_count
            all_data.append(df)

        log_message("info", "Waiting 12 seconds to respect API rate limits")
        time.sleep(12)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.drop_duplicates(subset=["Symbol", "Date"], inplace=True)
        log_message("info", f"Total rows extracted: {total_rows}")

        combined_df.to_csv("stock_data.csv", index=False)
        log_message("info", "Saved data to stock_data.csv")
    else:
        log_message("error", "No data was fetched for any symbols")

if __name__ == "__main__":
    main()
