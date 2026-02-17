# field_data_processor.py

import logging
import pandas as pd
from sqlalchemy import create_engine, text


class FieldDataProcessor:
    """Class to process field-related data from SQLite database and CSVs."""

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initialize the FieldDataProcessor class.
        
        Args:
            config_params (dict): Configuration dictionary with DB paths, queries, and mappings.
            logging_level (str): Logging level ("INFO", "DEBUG", "NONE").
        """
        self.db_path = config_params["db_path"]
        self.sql_query = config_params["sql_query"]
        self.columns_to_rename = config_params["columns_to_rename"]
        self.values_to_rename = config_params["values_to_rename"]
        self.weather_map_data = config_params["weather_mapping_csv"]

        self.df = None
        self.engine = None
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """Setup logging for this instance."""
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO

        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def create_db_engine(self):
        """Create a SQLAlchemy engine for the SQLite database."""
        self.engine = create_engine(f"sqlite:///{self.db_path}")
        self.logger.info("Database engine created successfully.")

    def ingest_sql_data(self):
        """Load data from SQLite DB into a DataFrame."""
        if self.engine is None:
            self.create_db_engine()
        with self.engine.connect() as connection:
            self.df = pd.read_sql_query(text(self.sql_query), connection)
        self.logger.info("Successfully loaded data.")
        return self.df

    def rename_columns(self):
        """Swap column names according to the configuration."""
        if self.df is None:
            self.logger.warning("DataFrame is empty. Cannot rename columns.")
            return

        col1, col2 = list(self.columns_to_rename.keys())[0], list(
            self.columns_to_rename.values()
        )[0]

        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        self.df.rename(columns={col1: temp_name, col2: col1}, inplace=True)
        self.df.rename(columns={temp_name: col2}, inplace=True)

        self.logger.info(f"Swapped columns: {col1} with {col2}")

    def apply_corrections(self, column_name="Crop_type", abs_column="Elevation"):
        """Apply crop name corrections and absolute value transformation."""
        if self.df is None:
            self.logger.warning("DataFrame is empty. Cannot apply corrections.")
            return

        # Absolute values for numeric column
        self.df[abs_column] = self.df[abs_column].abs()

        # Correct crop names
        self.df[column_name] = self.df[column_name].apply(
            lambda crop: self.values_to_rename.get(crop, crop)
        )

        self.logger.info("Applied crop name corrections and numeric corrections.")

    def weather_station_mapping(self):
        """Merge weather mapping data from a CSV URL."""
        if self.df is None:
            self.logger.warning("DataFrame is empty. Cannot map weather stations.")
            return

        weather_mapping_df = pd.read_csv(self.weather_map_data)
        self.df = self.df.merge(weather_mapping_df, on="Field_ID", how="left")
        self.logger.info("Weather station mapping applied successfully.")

    def process(self):
        """Run all processing steps in order."""
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()
        self.logger.info("All processing steps completed.")
