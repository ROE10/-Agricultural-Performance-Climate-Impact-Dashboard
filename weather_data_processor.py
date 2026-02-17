# weather_data_processor.py

import re
import logging
import numpy as np
import pandas as pd
from data_ingestion import read_from_web_CSV


class WeatherDataProcessor:
    def __init__(self, config_params, logging_level="INFO"):
        self.weather_station_data = config_params["weather_csv_path"]
        self.patterns = config_params["regex_patterns"]
        self.weather_df = None

        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        #Initialize logger for the class.
        logger_name = __name__ + ".WeatherDataProcessor"
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

    def load_weather_data(self):
        # Load weather station data from the web.
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data.")

    def extract_measurement(self, message):
    
        # Extract measurement type and value from message text.
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                value = float(
                    next((x for x in match.groups() if x is not None))
                )
                self.logger.debug("Extracted %s: %s", key, value)
                return key, value

        return None, None

    def process_messages(self):
        # Apply regex extraction to all messages.
        if self.weather_df is None:
            self.logger.warning("Weather data not loaded.")
            return None

        result = self.weather_df["Message"].apply(
            self.extract_measurement
        )

        self.weather_df["Measurement"], self.weather_df["Value"] = zip(*result)

        self.logger.info("Message processing completed.")
        return self.weather_df

    def calculate_means(self):
        # Calculate mean measurement values per station.
        if self.weather_df is None:
            self.logger.warning("Weather data not loaded.")
            return None

        means = (
            self.weather_df
            .groupby(["Weather_station_ID", "Measurement"])["Value"]
            .mean()
            .unstack()
        )

        self.logger.info("Mean values calculated.")
        return means

    def process(self):
        # Execute full weather data processing pipeline.
        self.load_weather_data()
        self.process_messages()
        self.logger.info("Weather data processing completed.")
