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
from geoglows_ecflow.workflow.suites.comfies.sdeploy import sdeploy


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


def create(config_path: str, target_files=[], params_values={}, dry_run=False) -> None:
    """Create the ecflow job definition file and its file structure.

    Args:
        config_path (str): Path to the configuration file.
        target_files (list): list of files to deploy; if empty, all files will be deployed
        params_values (dict): param/value dict; these override params from config file
        dry_run (bool): if True, no actual files will be created in target dir; useful for testing

    Returns:
        None
        
    Raises:
        DeployError: deploying the suite failed
    """
    sdeploy(config_path, target_files=target_files, params_values=params_values, dry_run=dry_run)


if __name__ == "__main__":  # pragma: no cover
    create(sys.argv[1])
