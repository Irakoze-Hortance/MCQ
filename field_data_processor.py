import logging
import pandas as pd
from data_ingestion import create_db_engine, query_data, read_from_web_CSV

class FieldDataProcessor:
    def __init__(self, config_params, logging_level="INFO"):
        """
        Initialize the FieldDataProcessor class.

        Parameters:
        config_params (dict): A dictionary containing configuration parameters.
        logging_level (str): Logging level (default is "INFO").
        """
        self.config_params = config_params
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        self.initialize_logging(logging_level)

        self.df = None
        self.engine = None
    
    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.

        Parameters:
        logging_level (str): Logging level.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def ingest_sql_data(self):
        """
        Ingest data from SQL database.
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Successfully loaded data.")
        return self.df
    
    def rename_columns(self):
        """
        Rename columns in the DataFrame.
        """
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]       

        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
        self.logger.info(f"Swapped columns: {column1} with {column2}")

        pass
            
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Apply corrections to DataFrame columns.

        Parameters:
        column_name (str): Name of the column to apply corrections to (default is 'Crop_type').
        abs_column (str): Name of the column to take absolute values of (default is 'Elevation').
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))
        pass

    def weather_station_mapping(self):
        """
        Map weather station data to the main DataFrame.
        """
        weather_mapping_df = read_from_web_CSV(self.weather_map_data)
        self.df.rename(columns={'Weather_station_ID': 'Weather_station'}, inplace=True)
        self.df = self.df.merge(weather_mapping_df, on='Field_ID', how='left')
        pass
    
    def process(self):
        """
        Execute data processing pipeline.
        """
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()
        pass
