import sys
import os
import json
from glob import glob
from geoglows_ecflow.resources.helper_functions import (
    create_logger,
    get_valid_vpucode_list,
    get_ensemble_number_from_forecast,
)

logger = create_logger("prep_task_log")


def rapid_forecast_preprocess(
    suite_home: str,
    rapid_input: str,
    rapid_output: str,
    runoff_dir: str,
    initialize_flows: bool = True
) -> list[tuple]:
    """Creates a dict of jobs to run.

    Args:
        suite_home (str): Path where the suite files will be created.
        rapid_input (str): Path to the rapid input.
        rapid_output (str): Path to the rapid output.
        runoff_dir (str): Path to runoff base directory containing ensemble
            runoff files. E.g. '/path/to/runoff_dir'
        initialize_flows (bool, optional): Whether to initialize flows.

    Returns:
        dict[dict]: dict of jobs to run.
    """

    # Create master dict
    master_dict = {
        "input_dir": rapid_input,
        "output_dir": rapid_output,
        "runoff_dir": runoff_dir,
        "date": os.path.basename(runoff_dir)
    }

    # Get list of rapid vpu input directories
    rapid_vpu_input_dirs = get_valid_vpucode_list(rapid_input)

    # Get list of ensemble runoff files
    ensemble_runoff_list = glob(os.path.join(runoff_dir, "*.runoff.*nc"))

    # Make the largest files first
    ensemble_runoff_list.sort(
        key=lambda x: int(os.path.basename(x).split(".")[0]), reverse=True
    )  # key=os.path.getsize

    # submit jobs to downsize ecmwf files to vpu
    for vpu in rapid_vpu_input_dirs:
        logger.info(f"Adding rapid input directory {vpu}")

        # get vpu-specific input directory
        master_vpu_input_dir = os.path.join(rapid_input, vpu)

        # create output directory if not exist
        if not os.path.exists(rapid_output):
            os.makedirs(rapid_output)

        # create jobs
        for runoff in ensemble_runoff_list:
            ensemble_number = get_ensemble_number_from_forecast(runoff)

            # get output file names
            outflow_file_name = f"Qout_{vpu}_{ensemble_number}.nc"

            # get output full path
            master_rapid_outflow_file = os.path.join(
                rapid_output, outflow_file_name
            )

            # add job to master dict
            master_dict[f"job_{vpu}_{ensemble_number}"] = {
                "runoff": runoff,
                "vpu": vpu,
                "ensemble": ensemble_number,
                "input_dir": master_vpu_input_dir,
                "output_file": master_rapid_outflow_file,
                "init_flows": initialize_flows
            }

    with open(os.path.join(suite_home, "rapid_run.json"), "w") as f:
        json.dump(master_dict, f)

    return master_dict


if __name__ == "__main__":
    rapid_forecast_preprocess(
        suite_home=sys.argv[1],
        rapid_input=sys.argv[2],
        rapid_output=sys.argv[3],
        runoff_dir=sys.argv[4]
    )
