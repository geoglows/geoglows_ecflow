import sys
import os
import json
from glob import glob
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
    """Creates a dict of jobs to run.

    Args:
        rapid_io_files_location (str): Path to the rapid io files.
        ecmwf_forecast_location (str): Path to the ecmwf forecast files.
        file_output_location (str): Path to the output file.

    Returns:
        dict[dict]: dict of jobs to run.
    """

    # Get list of rapid input directories
    rapid_input_directories = get_valid_vpucode_list(
            os.path.join(rapid_io_files_location, "input")
    )

    # Get list of runoff directories to run
    ecmwf_folders = sorted(glob(ecmwf_forecast_location))

    master_job_dict = {}

    for ecmwf_folder in ecmwf_folders:
        # get list of forecast files
        ensemble_runoff_list = glob(os.path.join(ecmwf_folder, '*.runoff.*nc'))

        # make the largest files first
        ensemble_runoff_list.sort(
            key=lambda x: int(os.path.basename(x).split('.')[0]),
            reverse=True
        )  # key=os.path.getsize
        forecast_date_timestep = get_date_timestep_from_forecast_dir(
            ecmwf_folder,
            logger
        )

        # submit jobs to downsize ecmwf files to vpu
        for rapid_input_directory in rapid_input_directories:

            logger.info(f'Adding rapid input folder {rapid_input_directory}')

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
            for runoff in ensemble_runoff_list:
                ensemble_number = get_ensemble_number_from_forecast(runoff)

                # get basin names
                outflow_file_name = 'Qout_%s_%s.nc' % (rapid_input_directory,
                                                       ensemble_number)

                master_rapid_outflow_file = os.path.join(
                        master_vpu_outflow_directory, outflow_file_name)

                job_id = ( f'job_{rapid_input_directory}_{ensemble_number}')

                master_job_dict[job_id] = {
                    'date': forecast_date_timestep,
                    'runoff': runoff,
                    'vpu': rapid_input_directory,
                    'ensemble': ensemble_number,
                    'input_dir': master_vpu_input_directory,
                    'output_file': master_rapid_outflow_file,
                    'init_flows': initialize_flows
                }

    with open(os.path.join(file_output_location, 'rapid_run.json'), 'w') as f:
        json.dump(master_job_dict, f)

    return master_job_dict


if __name__ == "__main__":
    ecmwf_rapid_process(
        rapid_io_files_location=sys.argv[1],
        ecmwf_forecast_location=sys.argv[2],
        file_output_location=sys.argv[3]
    )
