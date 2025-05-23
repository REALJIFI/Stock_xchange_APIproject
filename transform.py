import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from log import log_message

# Load environment variables
load_dotenv()

# Directory containing the dataset
DATASET_DIR = "dataset"
INPUT_FILE_NAME = "extracted_stock_data.csv"  # Replace with the name of your raw dataset file
OUTPUT_FILE_NAME = "transformed_data.csv"

def load_existing_data(file_name):
    """
    Load existing stock data from the dataset directory.

    Args:
        file_name (str): The name of the file to load.

    Returns:
        pd.DataFrame: DataFrame with loaded data, or None if the file doesn't exist.
    """
    file_path = os.path.join(DATASET_DIR, file_name)

    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        log_message("error", f"File {file_path} is missing or empty.")
        return None

    try:
        df = pd.read_csv(file_path)
        log_message("info", f"Loaded data from {file_path}")
        return df
    except Exception as e:
        log_message("error", f"Error loading data from {file_path}: {e}")
        return None

def transform_data(df):
    """
    Transform the stock data to a standardized format.

    Args:
        df (pd.DataFrame): DataFrame with raw stock data.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    try:
        # Ensure required columns are present
        expected_columns = ["Date", "OpenPrice", "High", "Low", "ClosePrice", "Volume", "Symbol"]
        if not all(col in df.columns for col in expected_columns):
            log_message("error", f"Dataset missing required columns. Found: {df.columns}")
            return pd.DataFrame()

        # Standardize column data types
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")  # Standardize date format
        df["OpenPrice"] = pd.to_numeric(df["OpenPrice"], errors="coerce")
        df["High"] = pd.to_numeric(df["High"], errors="coerce")
        df["Low"] = pd.to_numeric(df["Low"], errors="coerce")
        df["ClosePrice"] = pd.to_numeric(df["ClosePrice"], errors="coerce")
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce", downcast="integer")
        df["Symbol"] = df["Symbol"].str.upper()  # Ensure symbols are uppercase

        # Sort by date for consistency
        df.sort_values(by=["Date"], ascending=True, inplace=True)

        # Calculate DailyReturn
        df["DailyReturn"] = df.groupby("Symbol")["ClosePrice"].pct_change()

        log_message("info", "Data transformed to standard format, including DailyReturn")
        return df
    except Exception as e:
        log_message("error", f"Error transforming data: {e}")
        return pd.DataFrame()

def save_to_directory(df, file_name):
    """
    Save a DataFrame to a file in the dataset directory.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        file_name (str): The name of the file to save.
    """
    try:
        # Ensure the directory exists
        os.makedirs(DATASET_DIR, exist_ok=True)

        # Save file to the dataset directory
        file_path = os.path.join(DATASET_DIR, file_name)
        df.to_csv(file_path, index=False)

        log_message("info", f"File saved to {file_path}")
        print(f"File saved to {file_path}")
    except Exception as e:
        log_message("error", f"Error saving file: {e}")

def main():
    """
    Main function to load, transform, and save stock data.
    """
    log_message("info", "Starting data transformation process")
    
    # Load existing data
    raw_data = load_existing_data(INPUT_FILE_NAME)
    
    if raw_data is not None:
        # Transform data to standard format
        transformed_data = transform_data(raw_data)

        if not transformed_data.empty:
            # Save the transformed data
            save_to_directory(transformed_data, OUTPUT_FILE_NAME)
        else:
            log_message("error", "Transformed data is empty. No output file created.")
    else:
        log_message("error", "No raw data available for transformation.")

if __name__ == "__main__":
    main()
