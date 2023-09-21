import sys
import os
import re
from glob import glob
from datetime import datetime as dt
import logging as log

log.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=log.INFO
)


def get_date_timestep_from_forecast_dir(forecast_dir: str) -> str:
    """Gets the datetime from a forecast directory.

    Args:
        forecast_dir (str): Path representing forecast directory.

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


def get_ensemble_number_from_forecast(forecast_name):
    """
    Gets the datetimestep from forecast
    """
    forecast_split = os.path.basename(forecast_name).split(".")
    if forecast_name.endswith(".205.runoff.grib.runoff.netcdf"):
        ensemble_number = int(forecast_split[2])
    else:
        ensemble_number = int(forecast_split[0])
    return ensemble_number


def ecmwf_rapid_process(
    rapid_io_files_location="",
    ecmwf_forecast_location="",
    file_output_location="",
    region=""
):

    # Get list of rapid input directories
    rapid_input_directories = get_valid_vpucode_list(
            os.path.join(rapid_io_files_location, "input")
    )

    # Get list of runoff directories to run
    ecmwf_folders = sorted(glob(ecmwf_forecast_location))

    master_job_list = []

    for ecmwf_folder in ecmwf_folders:
        # get list of forecast files
        ecmwf_forecasts = glob(os.path.join(ecmwf_folder,
                                            '*.runoff.%s*nc' % region))

        # make the largest files first
        ecmwf_forecasts.sort(key=lambda x: int(os.path.basename(x).split('.')[0]), reverse=True)  # key=os.path.getsize
        forecast_date_timestep = get_date_timestep_from_forecast_dir(
                ecmwf_folder)

        # submit jobs to downsize ecmwf files to vpu
        rapid_watershed_jobs = {}
        for rapid_input_directory in rapid_input_directories:

            log.info(f'Adding rapid input folder {rapid_input_directory}')
            # keep list of jobs
            rapid_watershed_jobs[rapid_input_directory] = {
                'jobs': []
            }

            master_watershed_input_directory = os.path.join(
                    rapid_io_files_location,
                    "input",
                    rapid_input_directory)

            master_watershed_outflow_directory = os.path.join(
                    rapid_io_files_location,
                    'output',
                    rapid_input_directory,
                    forecast_date_timestep)

            try:
                os.makedirs(master_watershed_outflow_directory)
            except OSError:
                pass

            initialize_flows = True

            # create jobs
            for watershed_job_index, forecast in enumerate(ecmwf_forecasts):
                ensemble_number = get_ensemble_number_from_forecast(forecast)

                # get basin names
                outflow_file_name = 'Qout_%s_%s.nc' % (rapid_input_directory,
                                                       ensemble_number)

                master_rapid_outflow_file = os.path.join(
                        master_watershed_outflow_directory, outflow_file_name)

                job_name = 'job_%s_%s_%s' % (forecast_date_timestep,
                                             rapid_input_directory,
                                             ensemble_number)

                rapid_watershed_jobs[rapid_input_directory]['jobs'].append((
                        forecast,
                        forecast_date_timestep,
                        rapid_input_directory,
                        initialize_flows,
                        job_name,
                        master_rapid_outflow_file,
                        master_watershed_input_directory,
                        watershed_job_index
                ))

            master_job_list += rapid_watershed_jobs[rapid_input_directory]['jobs']


#    print(master_job_list)
    with open(os.path.join(file_output_location, 'rapid_run.txt'), 'w') as f:
        for line in master_job_list:
            formatted_line = ','.join(map(str, line))
            f.write(f"{formatted_line}\n")
    return master_job_list


# ------------------------------------------------------------------------------
# main process
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    ecmwf_rapid_process(
        rapid_io_files_location=str(sys.argv[1]),
        ecmwf_forecast_location=str(sys.argv[2]),
        file_output_location=str(sys.argv[3])
    )
