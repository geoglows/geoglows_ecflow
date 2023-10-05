# Original code by Alan Snow, 2017 (released under BSD 3-Clause License)
# See: spt_compute (https://github.com/erdc/spt_compute)
# Updated by Michael Souffront, 2023

import os
import sys
import re
import logging as log
from glob import glob


def create_logger(
    name: str,
    level: str = 'INFO',
    log_file: str | None = None
) -> log.Logger:
    # Create a logger
    logger = log.getLogger(name)
    logger.setLevel(level)

    if log_file:
        # Create file handler
        handler = log.FileHandler(log_file)
    else:
        handler = log.StreamHandler(sys.stdout)

        handler.setLevel(level)
        handler.setFormatter(
            log.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )

        # Add the handler to the logger
        logger.addHandler(handler)

    return logger


def get_date_from_forecast_dir(
    forecast_dir: str,
    log: log.Logger = create_logger(__name__)
) -> str:
    """Gets the datetime from a forecast directory.

    Args:
        forecast_dir (str): Path representing forecast directory.
        log (log.Logger, optional): Logger. Defaults to log.

    Returns:
        A string representing the datetime in the format "%Y%m%d.%H".

    Raises:
        AttributeError: If forecast_dir is not found or in the expected format.
    """
    # Get the forecast datetime from the forecast directory
    forecast_date_timestep = os.path.basename(forecast_dir)
    log.info(f'Forecast timestep {forecast_date_timestep}')

    # Parse forecast datetime
    date_time_regex = '\d{8}\.\d{2}'
    try:
        match = re.search(date_time_regex, forecast_date_timestep)
        date_time_str = match.group()
        return date_time_str
    except AttributeError:
        raise AttributeError("No datetime match found.")


def get_valid_vpucode_list(input_directory: str) -> list[str]:
    """
    Get a list of vpucodes from the input directory.

    Args:
        input_directory (str): Path to the rapid input directory.

    Returns:
        list[str]: List of valid directories (vpucodes).
    """
    valid_input_directories = []
    # Append to valid_input_directories if directory, skip if not
    for directory in os.listdir(input_directory):
        if os.path.isdir(os.path.join(input_directory, directory)):
            valid_input_directories.append(directory)
        else:
            print(f"{directory} not a directory. Skipping...")
    return valid_input_directories


def find_current_rapid_output(forecast_directory, vpucode):
    """
    Finds the most current files output from RAPID
    """
    if os.path.exists(forecast_directory):
        basin_files = glob(
            os.path.join(forecast_directory, f"Qout_{vpucode}_*.nc")
        )
        if len(basin_files) > 0:
            return basin_files
    # there are none found
    return None


def get_ensemble_number_from_forecast(forecast_name: str) -> int:
    """Gets the datetimestep from forecast.

    Args:
        forecast_name (str): Path to or name of the forecast file.
            E.g. "1.runoff.nc".

    Returns:
        int: The ensemble number.
    """
    forecast_split = os.path.basename(forecast_name).split(".")
    if forecast_name.endswith(".205.runoff.grib.runoff.netcdf"):
        ensemble_number = int(forecast_split[2])
    else:
        ensemble_number = int(forecast_split[0])
    return ensemble_number


def case_insensitive_file_search(directory: str, pattern: str) -> str:
    """Looks for file with pattern with case insensitive search.

    Args:
        directory (str): Path to directory to search.
        pattern (str): Pattern to search for.

    Returns:
        (str): Path to file with pattern.
    """
    try:
        file_path = os.path.join(
            directory,
            [filename for filename in os.listdir(directory)
                if re.search(pattern, filename, re.IGNORECASE)][0]
        )

        return file_path
    except IndexError:
        print(f"{pattern} not found")
        raise
