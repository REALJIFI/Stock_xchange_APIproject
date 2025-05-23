import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Constants
DATASET_DIR = os.getenv("DATASET_DIR", "dataset")
AGGREGATED_OUTPUT_DIR = os.getenv("AGGREGATED_OUTPUT_DIR", os.path.join(DATASET_DIR, "aggregated_data"))
TRANSFORMED_PREFIX = os.getenv("TRANSFORMED_PREFIX", "transformed_data")
AGGREGATED_PREFIX = os.getenv("AGGREGATED_PREFIX", "aggregated_data")

# Ensure directories exist
os.makedirs(DATASET_DIR, exist_ok=True)
os.makedirs(AGGREGATED_OUTPUT_DIR, exist_ok=True)

def get_latest_file(directory, prefix="transformed_data", extension=".csv"):
    """
    Get the latest file from a directory based on the modified timestamp.
    """
    try:
        files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(extension)]
        if not files:
            logger.error(f"No files with prefix '{prefix}' and extension '{extension}' found in {directory}")
            return None

        latest_file = max(
            files,
            key=lambda x: os.path.getmtime(os.path.join(directory, x))
        )
        return os.path.join(directory, latest_file)

    except Exception as e:
        logger.error(f"Error fetching latest file: {e}")
        return None

def save_with_timestamp(df, output_directory, prefix="aggregated_data"):
    """
    Save the DataFrame to a CSV file with a timestamped filename.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.csv"
        file_path = os.path.join(output_directory, filename)
        df.to_csv(file_path, index=False)
        logger.info(f"Aggregated data saved to {file_path}")
        return file_path

    except Exception as e:
        logger.error(f"Error saving aggregated file: {e}")
        return None

def aggregate_data(transformed_data):
    """
    Perform aggregation on transformed stock data.
    """
    try:
        # Log available columns for debugging
        logger.info(f"Available columns in data: {list(transformed_data.columns)}")

        # Required columns for aggregation
        required_columns = {'date', 'closeprice', 'volume', 'dailyreturn', 'symbol'}
        transformed_data.columns = transformed_data.columns.str.lower()  # Normalize column names
        missing_columns = required_columns - set(transformed_data.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

        # Fill missing values
        transformed_data['dailyreturn'] = transformed_data['dailyreturn'].fillna(0)

        # Find the maximum date for each symbol
        max_date = transformed_data.groupby('symbol')['date'].max().reset_index()
        max_date.rename(columns={'date': 'maxdate'}, inplace=True)

        # Group-level calculations
        company_performance_summary = transformed_data.groupby('symbol').agg(
            averagedailyprice=('closeprice', 'mean'),
            totalvolume=('volume', 'sum'),
            dailyreturn=('dailyreturn', 'mean')
        ).reset_index()

        volatility_index = transformed_data.groupby('symbol').agg(
            volatilityindex=('dailyreturn', 'std')
        ).reset_index()

        # Calculate moving averages
        moving_averages = transformed_data.groupby('symbol').apply(
            lambda group: pd.Series({
                'movingavg5day': group['closeprice'].rolling(window=5).mean().iloc[-1]
                if len(group) >= 5 else None,
                'movingavg10day': group['closeprice'].rolling(window=10).mean().iloc[-1]
                if len(group) >= 10 else None,
            })
        ).reset_index()

        # Merge results
        aggregated_data = company_performance_summary.merge(volatility_index, on='symbol', how='left')
        aggregated_data = aggregated_data.merge(moving_averages, on='symbol', how='left')
        aggregated_data = aggregated_data.merge(max_date, on='symbol', how='left')

        logger.info("Data aggregation completed successfully.")
        return aggregated_data

    except Exception as e:
        logger.error(f"Error during data aggregation: {e}")
        return pd.DataFrame()



def main():
    """
    Main function to aggregate transformed data.
    """
    logger.info("Starting aggregation process.")

    # Step 1: Get the latest transformed data file
    latest_file_path = get_latest_file(DATASET_DIR, prefix=TRANSFORMED_PREFIX)
    if not latest_file_path:
        logger.error("No transformed data file available for aggregation.")
        return

    try:
        transformed_data = pd.read_csv(latest_file_path)

        # Step 2: Perform aggregation
        aggregated_data = aggregate_data(transformed_data)

        if not aggregated_data.empty:
            # Step 3: Save aggregated data with a timestamped filename
            save_with_timestamp(aggregated_data, AGGREGATED_OUTPUT_DIR, prefix=AGGREGATED_PREFIX)
        else:
            logger.error("Aggregated data is empty. No file was saved.")

    except Exception as e:
        logger.error(f"Error in aggregation process: {e}")

if __name__ == "__main__":
    main()
