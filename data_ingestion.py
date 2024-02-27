### START FUNCTION

"""
This module contains functions for ingesting data from various sources into a SQLite database.

The module includes functions for:

* Creating a SQLite database connection
* Executing a SQL query against a SQLite database
* Loading data from a CSV file into a SQLite database

"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text

# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger("data_ingestion")

# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def create_db_engine(db_path):
    """Creates and returns a database engine object for the given database path.

    Parameters:
    db_path (str): The path to the database file.

    Returns:
    engine (sqlalchemy.engine.Engine): The database engine object.

    Raises:
    ImportError: If SQLAlchemy is not installed.
    Exception: If the database engine creation fails for any other reason.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine  # Return the engine object if it all works well
    except (
        ImportError
    ):  # If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error(
            "SQLAlchemy is required to use this function. Please install it first."
        )
        raise e
    except Exception as e:  # If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e


def query_data(engine, sql_query):
    """Queries data from the database using the given engine and query.

    Parameters:
    engine (sqlalchemy.engine.Engine): The database engine object.
    sql_query (str): The SQL query to execute.

    Returns:
    df (pandas.DataFrame): The DataFrame containing the query results.

    Raises:
    ValueError: If the query returns an empty DataFrame.
    Exception: If the query fails for any other reason.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e:
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e


def read_from_web_CSV(URL):
    """Reads data from a CSV file on the web and returns a DataFrame.

    Parameters:
    URL (str): The URL of the CSV file.

    Returns:
    df (pandas.DataFrame): The DataFrame containing the CSV data.

    Raises:
    pd.errors.EmptyDataError: If the URL does not point to a valid CSV file.
    Exception: If the CSV reading fails for any other reason.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error(
            "The URL does not point to a valid CSV file. Please check the URL and try again."
        )
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e


### END FUNCTION
