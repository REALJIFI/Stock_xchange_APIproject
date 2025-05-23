import os
import pandas as pd
from extract import fetch_stock_data, SYMBOLS  # Import fetch function and symbols
from transform import transform_data
from load import create_schemas_and_tables, load_data_to_postgres, move_data_to_edw
from aggregated import aggregate_data, save_with_timestamp
from log import log_message
from datetime import datetime


def ensure_directory_exists(directory):
    """
    Ensure the specified directory exists. If not, create it.

    Args:
        directory (str): Path to the directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        log_message("info", f"Directory '{directory}' created.")


def extract_data():
    """
    Extract data for multiple stock symbols.

    Returns:
        pd.DataFrame: Combined DataFrame containing stock data for all symbols.
    """
    log_message("info", "Starting data extraction process")

    all_data = []  # List to collect data for all symbols
    total_rows = 0  # Total rows extracted

    for symbol in SYMBOLS:
        # Fetch data for the symbol
        df = fetch_stock_data(symbol)

        if df is not None:
            # Log and append the data
            row_count = len(df)
            log_message("info", f"Extracted {row_count} rows for {symbol}")
            total_rows += row_count
            all_data.append(df)

    # Combine all data into a single DataFrame
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        log_message("info", f"Total rows extracted: {total_rows}")
        return combined_df

    log_message("error", "No data was fetched for any symbols")
    return pd.DataFrame()  # Return an empty DataFrame if no data


def main():
    """
    Main ETL function orchestrating the pipeline.
    """
    log_message("info", "ETL process started")

    # Step 1: Schema Setup
    log_message("info", "Creating schemas and tables")
    create_schemas_and_tables()

    # Ensure the dataset directory exists
    dataset = "dataset"
    ensure_directory_exists(dataset)

    # Step 2: Extract
    log_message("info", "Starting extraction phase")
    extracted_data = extract_data()

    if not extracted_data.empty:
        # Save extracted data
        extracted_file = save_with_timestamp(extracted_data, dataset, "extracted_stock_data")

        # Step 3: Transform
        log_message("info", "Starting transformation phase")
        transformed_data = transform_data(extracted_data)

        if not transformed_data.empty:
            # Save transformed data
            transformed_file = save_with_timestamp(transformed_data, dataset, "transformed_data")

            # Step 4: Load
            log_message("info", "Starting load phase")
            load_data_to_postgres(transformed_file)

            # Step 5: Move to EDW
            log_message("info", "Moving data to EDW")
            move_data_to_edw()

            # Step 6: Aggregate
            log_message("info", "Starting aggregation phase")
            aggregated_data = aggregate_data(transformed_data)

            if not aggregated_data.empty:
                # Save aggregated data
                aggregated_file = save_with_timestamp(aggregated_data, dataset, "aggregated_data")
                log_message("info", f"Aggregated data saved to: {aggregated_file}")
            else:
                log_message("error", "Aggregation failed. No aggregated data produced.")

            log_message("info", "ETL pipeline completed successfully.")
        else:
            log_message("error", "Transformation failed. ETL process terminated.")
    else:
        log_message("error", "Extraction failed. ETL process terminated.")

    log_message("info", "ETL process completed")


if __name__ == "__main__":
    main()
