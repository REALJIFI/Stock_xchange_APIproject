import pandas as pd
import psycopg2
from psycopg2 import sql
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

STG_SCHEMA = "stg"
EDW_SCHEMA = "edw"
STG_TABLE = "stock_data"
EDW_TABLE = "stock_data"

def create_schemas_and_tables():
    """
    Create schemas and tables for staging (stg) and enterprise data warehouse (edw).
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            user=os.getenv("P_user"),
            password=os.getenv("P_password"),
            host=os.getenv("P_host"),
            port=os.getenv("P_port"),
            dbname=os.getenv("P_database")
        )
        cursor = conn.cursor()

        # Create schemas
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {STG_SCHEMA};")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {EDW_SCHEMA};")

        # Create tables with unique constraints
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {STG_SCHEMA}.{STG_TABLE} (
                RecordID SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                open FLOAT NOT NULL CHECK (open >= 0),
                high FLOAT NOT NULL CHECK (high >= 0),
                low FLOAT NOT NULL CHECK (low >= 0),
                close FLOAT NOT NULL CHECK (close >= 0),
                volume BIGINT NOT NULL CHECK (volume >= 0),
                symbol VARCHAR(10) NOT NULL,
                UNIQUE (symbol, date)
            );
        """)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {EDW_SCHEMA}.{EDW_TABLE} (
                RecordID SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                open FLOAT NOT NULL CHECK (open >= 0),
                high FLOAT NOT NULL CHECK (high >= 0),
                low FLOAT NOT NULL CHECK (low >= 0),
                close FLOAT NOT NULL CHECK (close >= 0),
                volume BIGINT NOT NULL CHECK (volume >= 0),
                symbol VARCHAR(10) NOT NULL,
                UNIQUE (symbol, date)
            );
        """)

        conn.commit()
        logging.info("Schemas and tables created successfully with required constraints.")

    except Exception as e:
        logging.error(f"Error creating schemas or tables: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_data_into_staging(data, table_name, engine):
    """
    Load data into the specified staging table.
    """
    try:
        data.to_sql(table_name, con=engine, schema="stg", if_exists='append', index=False)
        logging.info(f"{len(data)} rows loaded into staging table: {table_name}.")
    except Exception as e:
        logging.error(f"Error loading data into staging table {table_name}: {e}")
        raise

def load_data_to_postgres(input_file="dataset\transformed_data_20241219_110050.csv"):
    """
    Loads the transformed data into the PostgreSQL staging table.

    Args:
        input_file (str): Path to the transformed data file.
    """
    logging.info("Starting data load process.")

    if not os.path.exists(input_file) or os.path.getsize(input_file) == 0:
        logging.error("Transformed data file is missing or empty.")
        raise FileNotFoundError("No data available for loading.")

    conn = None
    cursor = None



def move_data_to_edw():
    """
    Move data from the staging (stg) table to the enterprise data warehouse (edw) table.
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            user=os.getenv("P_user"),
            password=os.getenv("P_password"),
            host=os.getenv("P_host"),
            port=os.getenv("P_port"),
            dbname=os.getenv("P_database")
        )
        cursor = conn.cursor()

        # Move data from staging to EDW
        cursor.execute(f"""
            INSERT INTO {EDW_SCHEMA}.{EDW_TABLE} (date, open, high, low, close, volume, symbol)
            SELECT date, open, high, low, close, volume, symbol
            FROM {STG_SCHEMA}.{STG_TABLE}
            ON CONFLICT (symbol, date) DO UPDATE
            SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """)
        conn.commit()
        logging.info("Data moved to EDW table successfully.")

    except Exception as e:
        logging.error(f"Error moving data to EDW: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    # Create schemas and tables
    create_schemas_and_tables()

    # Load data into staging table
    load_data_to_postgres(r"dataset\transformed_data_20241219_110050.csv")

    # Move data to EDW table
    move_data_to_edw()

