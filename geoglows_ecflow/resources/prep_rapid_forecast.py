import sys
import os
import json
from glob import glob
from geoglows_ecflow.resources.helper_functions import (
    create_logger,
    get_date_from_forecast_dir,
    get_valid_vpucode_list,
    get_ensemble_number_from_forecast,
)

logger = create_logger("prep_task_log")


def rapid_forecast_preprocess(
    rapid_io_dir: str,
    runoff_base_dir: str,
    output_dir: str,
) -> list[tuple]:
    """Creates a dict of jobs to run.

    Args:
        rapid_io_dir (str): Path to the rapid io files.
        runoff_base_dir (str): Path to runoff files containing IFS.48r1.Runoff
            files. E.g. '/path/to/runoff_base_dir'
        output_dir (str): Path where the ecflow output files will be created.

    Returns:
        dict[dict]: dict of jobs to run.
    """
    # Get rapid io input and output
    rapid_io_input = os.path.join(rapid_io_dir, "input")
    rapid_io_output = os.path.join(rapid_io_dir, "output")

    # Create master dict
    master_dict = {
        "input_dir": rapid_io_input,
        "output_dir": rapid_io_output,
        "runoff_dir": runoff_base_dir,
        "dates": {},
    }

    # Get list of rapid vpu input directories
    rapid_vpu_input_dirs = get_valid_vpucode_list(rapid_io_input)

    # Get list of runoff directories to run
    runoff_dirs = sorted(
        glob(os.path.join(runoff_base_dir, "IFS.48r1.Runoff*.netcdf"))
    )

    # Loop through each runoff in case multiple dates available
    for runoff_dir in runoff_dirs:
        # get list of ensemble runoff files
        ensemble_runoff_list = glob(os.path.join(runoff_dir, "*.runoff.*nc"))

        # make the largest files first
        ensemble_runoff_list.sort(
            key=lambda x: int(os.path.basename(x).split(".")[0]), reverse=True
        )  # key=os.path.getsize
        date = get_date_from_forecast_dir(runoff_dir, logger)
        master_dict["dates"][date] = []

        # submit jobs to downsize ecmwf files to vpu
        for vpu in rapid_vpu_input_dirs:
            logger.info(f"Adding rapid input directory {vpu}")

            master_vpu_input_dir = os.path.join(rapid_io_dir, "input", vpu)

            master_vpu_outflow_dir = os.path.join(
                rapid_io_dir, "output", vpu, date
            )

            try:
                os.makedirs(master_vpu_outflow_dir)
            except OSError:
                pass

            initialize_flows = True

            # create jobs
            for runoff in ensemble_runoff_list:
                ensemble_number = get_ensemble_number_from_forecast(runoff)

                # get output file names
                outflow_file_name = f"Qout_{vpu}_{ensemble_number}.nc"

                master_rapid_outflow_file = os.path.join(
                    master_vpu_outflow_dir, outflow_file_name
                )

                job_id = f"job_{vpu}_{ensemble_number}"

                master_dict["dates"][date].append({
                    job_id: {
                        "runoff": runoff,
                        "vpu": vpu,
                        "ensemble": ensemble_number,
                        "input_dir": master_vpu_input_dir,
                        "output_file": master_rapid_outflow_file,
                        "init_flows": initialize_flows
                    }
                })

    with open(os.path.join(output_dir, "rapid_run.json"), "w") as f:
        json.dump(master_dict, f)

    return master_dict


if __name__ == "__main__":
    rapid_forecast_preprocess(
        rapid_io_dir=sys.argv[1],
        runoff_base_dir=sys.argv[2],
        output_dir=sys.argv[3]
    )
