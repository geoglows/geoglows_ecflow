import os
import sys
import json
from geoglows_ecflow.resources.helper_functions import (
    compute_initial_rapid_flows,
    find_current_rapid_output,
)


def compute_init_flows(ecflow_home: str, vpu: str) -> None:
    with open(os.path.join(ecflow_home, "rapid_run.json"), "r") as f:
        data = json.load(f)
        rapid_input_dir = data["input_dir"]
        rapid_output_dir = data["output_dir"]
        for date in data["dates"].keys():
            # Initialize flows for next run
            input_directory = os.path.join(rapid_input_dir, vpu)

            forecast_directory = os.path.join(rapid_output_dir, vpu, date)

            if os.path.exists(forecast_directory):
                basin_files = find_current_rapid_output(forecast_directory, vpu)
                try:
                    compute_initial_rapid_flows(
                        basin_files, input_directory, date
                    )
                except Exception as ex:
                    print(ex)
                    pass


if __name__ == "__main__":
    ecflow_home = sys.argv[1]
    vpu = sys.argv[2]
    compute_init_flows(ecflow_home, vpu)
