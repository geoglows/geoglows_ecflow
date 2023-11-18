import os
import sys
from ecflow import Defs, Family, Task
from geoglows_ecflow.utils import (
    load_config,
    prepare_dir_structure,
    create_symlinks_for_family_tasks,
    add_variables,
    validate,
)
from geoglows_ecflow.resources.helper_functions import get_valid_vpucode_list


def create_rapid_run_family(
    family_name: str,
    task_name: str,
    vpu_list: list[str],
    trigger_task: str,
    rapid_exec: str,
    rapid_exec_dir: str,
    rapid_subprocess_dir: str,
    is_local: bool = False,
) -> Family:
    """Create the ecflow family for the main rapid run.

    Args:
        family_name (str): Name of the family group.
        task_name (str): Name of the task.
        vpu_list (list[str]): List of VPU codes.
        trigger_task (str): Task that the family depends on.
        rapid_exec (str): Path to the rapid executable.
        rapid_exec_dir (str): Path to the rapid executable directory.
        rapid_subprocess_dir (str): Path to the rapid subprocess directory.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), "resources", "run_rapid_forecast.py"
    )

    family = Family(family_name)
    family.add_trigger(f"{trigger_task} == complete")
    family.add_variable("PYSCRIPT", pyscript_path)
    family.add_variable("RAPID_EXEC", rapid_exec)
    family.add_variable("EXEC_DIR", rapid_exec_dir)
    family.add_variable("SUBPROCESS_DIR", rapid_subprocess_dir)

    for i, vpu in enumerate(vpu_list):
        # Create the ensemble tasks
        for j in reversed(range(1, 53)):
            task = Task(f"{task_name}_{vpu}_{j}")
            task.add_variable("JOB_ID", f"job_{vpu}_{j}")
            if is_local:
                if i > 0 or j != 52:
                    prev_vpu = vpu_list[i - 1] if j + 1 > 52 else vpu
                    task.add_trigger(
                        f"{task_name}_{prev_vpu}_{j % 52 + 1} == complete"
                    )

            family.add_task(task)

    return family


def create_nc_to_zarr_family(
    family_name: str,
    task_name: str,
    vpu_list: list[str],
    trigger: str,
    is_local: bool = False,
) -> Family:
    """Create the ecflow family for converting netcdf forecast to zarr.

    Args:
        family_name (str): Name of the family group.
        task_name (str): Name of the task.
        vpu_list (list[str]): List of VPU codes.
        trigger (str): Trigger that will start this family.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), "resources", "netcdf_to_zarr.py"
    )

    family = Family(family_name)
    family.add_trigger(f"{trigger} == complete")
    family.add_variable("PYSCRIPT", pyscript_path)

    for i, vpu in enumerate(vpu_list):
        # Create init flows tasks
        task = Task(f"{task_name}_{vpu}")
        task.add_variable("VPU", vpu)
        if is_local:
            if i > 0:
                prev_vpu = vpu_list[i - 1]
                task.add_trigger(f"{task_name}_{prev_vpu} == complete")

        family.add_task(task)

    return family


def create_init_flows_family(
    family_name: str,
    task_name: str,
    vpu_list: list[str],
    trigger: str,
    is_local: bool = False,
) -> Family:
    """Create the ecflow family for initializing flows.

    Args:
        family_name (str): Name of the family group.
        task_name (str): Name of the task.
        vpu_list (list[str]): List of VPU codes.
        trigger (str): Trigger that will start this family.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), "resources", "compute_init_flows.py"
    )

    family = Family(family_name)
    family.add_trigger(f"{trigger} == complete")
    family.add_variable("PYSCRIPT", pyscript_path)

    for i, vpu in enumerate(vpu_list):
        # Create init flows tasks
        task = Task(f"{task_name}_{vpu}")
        task.add_variable("VPU", vpu)
        if is_local:
            if i > 0:
                prev_vpu = vpu_list[i - 1]
                task.add_trigger(f"{task_name}_{prev_vpu} == complete")

        family.add_task(task)

    return family


def create_esri_table_family(
    family_name: str,
    task_name: str,
    vpu_list: list[str],
    nces_exec: str,
    trigger: str,
    is_local: bool = False,
) -> Family:
    """Create the ecflow family for generating esri tables.

    Args:
        family_name (str): Name of the family group.
        task_name (str): Name of the task.
        vpu_list (list[str]): List of VPU codes.
        nces_exec (str): Path to the nces executable.
        trigger (str): Trigger that will start this family.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), "resources", "generate_esri_table.py"
    )

    family = Family(family_name)
    family.add_trigger(f"{trigger} == complete")
    family.add_variable("PYSCRIPT", pyscript_path)
    family.add_variable("NCES_EXEC", nces_exec)

    for i, vpu in enumerate(vpu_list):
        # Create init flows tasks
        task = Task(f"{task_name}_{vpu}")
        task.add_variable("VPU", str(vpu))
        if is_local:
            if i > 0:
                prev_vpu = vpu_list[i - 1]
                task.add_trigger(f"{task_name}_{prev_vpu} == complete")

        family.add_task(task)

    return family


def create_day_one_family(
    family_name: str,
    task_name: str,
    vpu_list: list[str],
    output_dir: str,
    trigger: str,
    is_local: bool = False,
) -> Family:
    """Create the ecflow family for forecast_records.

    Args:
        family_name (str): Name of the family group.
        task_name (str): Name of the task.
        vpu_list (list[str]): List of VPU codes.
        output_dir (str): Path to forecast records output directory.
        trigger (str): Trigger that will start this family.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), "resources", "day_one_forecast.py"
    )

    family = Family(family_name)
    family.add_trigger(f"{trigger} == complete")
    family.add_variable("PYSCRIPT", pyscript_path)

    for i, vpu in enumerate(vpu_list):
        # Create init flows tasks
        task = Task(f"{task_name}_{vpu}")
        task.add_variable("VPU", str(vpu))
        task.add_variable("OUTPUT_DIR", output_dir)
        if is_local:
            if i > 0:
                prev_vpu = vpu_list[i - 1]
                task.add_trigger(f"{task_name}_{prev_vpu} == complete")

        family.add_task(task)

    return family


def create_aws_family(
    family_name: str,
    task_name: str,
    vpu_list: list[str],
    aws_config: str,
    trigger: str,
    is_local: bool = False,
) -> Family:
    """Create the ecflow family for archiving zarr forecasts to aws.

    Args:
        family_name (str): Name of the family group.
        task_name (str): Name of the task.
        vpu_list (list[str]): List of VPU codes.
        aws_config (str): Path to aws configuration yaml file.
        trigger (str): Trigger that will start this family.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), "resources", "archive_to_aws.py"
    )

    family = Family(family_name)
    family.add_trigger(f"{trigger} == complete")
    family.add_variable("PYSCRIPT", pyscript_path)
    family.add_variable("AWS_CONFIG", aws_config)

    for i, vpu in enumerate(vpu_list):
        # Create init flows tasks
        task = Task(f"{task_name}_{vpu}")
        task.add_variable("VPU", vpu)
        if is_local:
            if i > 0:
                prev_vpu = vpu_list[i - 1]
                task.add_trigger(f"{task_name}_{prev_vpu} == complete")

        family.add_task(task)

    return family


def create(config_path: str) -> None:
    """Create the ecflow job definition file and its file structure.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        Defs: ecflow job definition.
    """
    # Load the configuration file
    config = load_config(config_path)
    python_exec = config["python_exec"]
    ecflow_home = config["ecflow_home"]
    ecflow_bin = config.get("ecflow_bin")
    workspace = config["workspace"]
    local_run = config.get("local_run")
    ecflow_entities = config["ecflow_entities"]
    ecflow_suite = config["ecflow_entities"]["suite"]["name"]
    ecflow_suite_logs = config["ecflow_entities"]["suite"]["logs"]
    rapid_family_name = config["ecflow_entities"]["family"][0]["name"]
    init_flows_family_name = config["ecflow_entities"]["family"][1]["name"]
    esri_table_family_name = config["ecflow_entities"]["family"][2]["name"]
    nc_to_zarr_family_name = config["ecflow_entities"]["family"][3]["name"]
    aws_family_name = config["ecflow_entities"]["family"][4]["name"]
    day_one_family_name = config["ecflow_entities"]["family"][5]["name"]
    prep_task_name = config["ecflow_entities"]["task"][0]["name"]
    esri_table_task_name = config["ecflow_entities"]["task"][1]["name"]
    day_one_task_name = config["ecflow_entities"]["task"][2]["name"]
    rapid_task_name = config["ecflow_entities"]["task"][3]["name"]
    init_flows_task_name = config["ecflow_entities"]["task"][4]["name"]
    nc_to_zarr_task_name = config["ecflow_entities"]["task"][5]["name"]
    aws_task_name = config["ecflow_entities"]["task"][6]["name"]
    rapid_exec = config["rapid_exec"]
    rapid_exec_dir = config["rapid_exec_dir"]
    rapid_subprocess_dir = config["rapid_subprocess_dir"]
    forecast_records_dir = config["forecast_records_dir"]
    nces_exec = config["nces_exec"]
    aws_config = config["aws_config"]

    # Get vpu list
    vpu_list = get_valid_vpucode_list(os.path.join(workspace, "input"))

    # Prepare the directory structure
    prepare_dir_structure(
        python_exec, ecflow_home, ecflow_entities, ecflow_suite_logs
    )

    # Create symlinks for all tasks
    task_family_list = [
        (rapid_task_name, rapid_family_name),
        (init_flows_task_name, init_flows_family_name),
        (esri_table_task_name, esri_table_family_name),
        (nc_to_zarr_task_name, nc_to_zarr_family_name),
        (day_one_task_name, day_one_family_name),
        (aws_task_name, aws_family_name),
    ]
    create_symlinks_for_family_tasks(
        ecflow_home, ecflow_suite, task_family_list, vpu_list
    )

    # Define the ecflow job
    defs = Defs()

    # Add the suite to the job
    suite = defs.add_suite(ecflow_suite)

    # Set variables for the suite
    suite_variables = {
        "ECF_INCLUDE": ecflow_home,
        "ECF_FILES": os.path.join(ecflow_home, ecflow_suite),
        "ECF_HOME": ecflow_home,
        "ECF_BIN": ecflow_bin if local_run else "",
        "WORKSPACE": workspace,
    }
    add_variables(suite, suite_variables)

    # Define 'prep_task'
    prep_task = suite.add_task(prep_task_name)

    # Set variables for 'prep_task'
    prep_task_ps = os.path.join(
        os.path.dirname(__file__), "resources", "prep_rapid_forecast.py"
    )
    prep_task_vars = {"PYSCRIPT": prep_task_ps}
    add_variables(prep_task, prep_task_vars)

    # Add the rapid run family to the suite
    rapid_run_family = create_rapid_run_family(
        rapid_family_name,
        rapid_task_name,
        vpu_list,
        prep_task_name,
        rapid_exec,
        rapid_exec_dir,
        rapid_subprocess_dir,
        is_local=local_run,
    )
    suite.add_family(rapid_run_family)

    # Add the esri table family to the suite
    esri_table_family = create_esri_table_family(
        esri_table_family_name,
        esri_table_task_name,
        vpu_list,
        nces_exec,
        rapid_family_name,
        is_local=local_run,
    )
    suite.add_family(esri_table_family)

    # Add the init flows family to the suite
    nc_to_zarr_family = create_nc_to_zarr_family(
        nc_to_zarr_family_name,
        nc_to_zarr_task_name,
        vpu_list,
        esri_table_family_name,
        is_local=local_run,
    )
    suite.add_family(nc_to_zarr_family)

    # Add the init flows family to the suite
    init_flows_family = create_init_flows_family(
        init_flows_family_name,
        init_flows_task_name,
        vpu_list,
        nc_to_zarr_family_name,
        is_local=local_run,
    )
    suite.add_family(init_flows_family)

    # Add the init flows family to the suite
    day_one_family = create_day_one_family(
        day_one_family_name,
        day_one_task_name,
        vpu_list,
        forecast_records_dir,
        init_flows_family_name,
        is_local=local_run,
    )
    suite.add_family(day_one_family)

    # Add the aws family to the suite
    aws_family = create_aws_family(
        aws_family_name,
        aws_task_name,
        vpu_list,
        aws_config,
        day_one_family_name,
        is_local=local_run,
    )
    suite.add_family(aws_family)

    # Validate definition job
    validate(defs)

    # Save the ecflow definition job
    output_path = os.path.join(ecflow_home, f"{ecflow_suite}.def")
    print(f"Saving definition to file '{output_path}'")
    defs.save_as_defs(output_path)

    return defs


if __name__ == "__main__":  # pragma: no cover
    create(sys.argv[1])
