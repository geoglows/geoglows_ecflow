import sys
import os
from glob import glob
import logging as log
from geoglows_ecflow.resources.helper_functions import (
    create_logger, get_date_timestep_from_forecast_dir, get_valid_vpucode_list,
    get_ensemble_number_from_forecast
)

logger = create_logger('prep_task_log')


def ecmwf_rapid_process(
    rapid_io_files_location: str,
    ecmwf_forecast_location: str,
    file_output_location: str,
) -> list[tuple]:
    """Creates a list of jobs to run.

    Args:
        rapid_io_files_location (str): Path to the rapid io files.
        ecmwf_forecast_location (str): Path to the ecmwf forecast files.
        file_output_location (str): Path to the output file.

    Returns:
        list[tuple]: List of jobs to run.
    """

    # Get list of rapid input directories
    rapid_input_directories = get_valid_vpucode_list(
            os.path.join(rapid_io_files_location, "input")
    )

    # Get list of runoff directories to run
    ecmwf_folders = sorted(glob(ecmwf_forecast_location))

    master_job_list = []

    for ecmwf_folder in ecmwf_folders:
        # get list of forecast files
        ecmwf_forecasts = glob(os.path.join(ecmwf_folder, '*.runoff.*nc'))

        # make the largest files first
        ecmwf_forecasts.sort(
            key=lambda x: int(os.path.basename(x).split('.')[0]),
            reverse=True
        )  # key=os.path.getsize
        forecast_date_timestep = get_date_timestep_from_forecast_dir(
            ecmwf_folder,
            logger
        )

        # submit jobs to downsize ecmwf files to vpu
        rapid_vpu_jobs = {}
        for rapid_input_directory in rapid_input_directories:

            logger.info(f'Adding rapid input folder {rapid_input_directory}')
            # keep list of jobs
            rapid_vpu_jobs[rapid_input_directory] = {
                'jobs': []
            }

            master_vpu_input_directory = os.path.join(
                    rapid_io_files_location,
                    "input",
                    rapid_input_directory)

            master_vpu_outflow_directory = os.path.join(
                    rapid_io_files_location,
                    'output',
                    rapid_input_directory,
                    forecast_date_timestep)

            try:
                os.makedirs(master_vpu_outflow_directory)
            except OSError:
                pass

            initialize_flows = True

            # create jobs
            for vpu_job_index, forecast in enumerate(ecmwf_forecasts):
                ensemble_number = get_ensemble_number_from_forecast(forecast)

                # get basin names
                outflow_file_name = 'Qout_%s_%s.nc' % (rapid_input_directory,
                                                       ensemble_number)

                master_rapid_outflow_file = os.path.join(
                        master_vpu_outflow_directory, outflow_file_name)

                job_name = 'job_%s_%s_%s' % (forecast_date_timestep,
                                             rapid_input_directory,
                                             ensemble_number)

                rapid_vpu_jobs[rapid_input_directory]['jobs'].append((
                        forecast,
                        forecast_date_timestep,
                        rapid_input_directory,
                        initialize_flows,
                        job_name,
                        master_rapid_outflow_file,
                        master_vpu_input_directory,
                        vpu_job_index
                ))
            master_job_list += rapid_vpu_jobs[rapid_input_directory] \
                ['jobs']

    with open(os.path.join(file_output_location, 'rapid_run.txt'), 'w') as f:
        for line in master_job_list:
            formatted_line = ','.join(map(str, line))
            f.write(f"{formatted_line}\n")
    return master_job_list


if __name__ == "__main__":
    ecmwf_rapid_process(
        rapid_io_files_location=sys.argv[1],
        ecmwf_forecast_location=sys.argv[2],
        file_output_location=sys.argv[3]
    )
