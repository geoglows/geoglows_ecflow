import os
import sys
from ecflow import Defs, Family, Task
from geoglows_ecflow.utils import (load_config, prepare_dir_structure,
                                   create_symlinks_for_family_tasks,
                                   add_variables, validate)
from geoglows_ecflow.resources.constants import VPU_LIST


def create_rapid_run_family(
    family_name: str,
    task_name: str,
    trigger_task: str,
    rapid_exec: str,
    rapid_exec_dir: str,
    rapid_subprocess_dir: str,
    is_local: bool = False
) -> Family:
    """Create the ecflow family for the main rapid run.

    Args:
        family_name (str): Name of the family group.
        task_name (str): Name of the task.
        trigger_task (str): Task that the family depends on.
        rapid_exec (str): Path to the rapid executable.
        rapid_exec_dir (str): Path to the rapid executable directory.
        rapid_subprocess_dir (str): Path to the rapid subprocess directory.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), 'resources', 'run_rapid_forecast.py'
    )

    family = Family(family_name)
    family.add_trigger(f"{trigger_task} == complete")
    family.add_variable("PYSCRIPT", pyscript_path)
    family.add_variable("RAPID_EXEC", rapid_exec)
    family.add_variable("EXEC_DIR", rapid_exec_dir)
    family.add_variable("SUBPROCESS_DIR", rapid_subprocess_dir)

    for i, vpu in enumerate(VPU_LIST):
        # Create the ensemble tasks
        for j in reversed(range(1, 53)):
            task = Task(f"{task_name}_{vpu}_{j}")
            task.add_variable("JOB_ID", f'job_{vpu}_{j}')
            if is_local:
                if i > 0 or j != 52:
                    prev_vpu = VPU_LIST[i - 1] if j + 1 > 52 else vpu
                    task.add_trigger(
                        f"{task_name}_{prev_vpu}_{j % 52 + 1} == complete"
                    )

            family.add_task(task)

    return family


def create_nc_to_zarr_family(
    family_name: str,
    task_name: str,
    trigger: str,
    is_local: bool = False
) -> Family:
    """Create the ecflow family for converting netcdf forecast to zarr.

    Args:
        family_name (str): Name of the family group.
        task_name (str): Name of the task.
        trigger (str): Trigger that will start this family.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), 'resources', 'netcdf_to_zarr.py'
    )

    family = Family(family_name)
    family.add_trigger(f"{trigger} == complete")
    family.add_variable("PYSCRIPT", pyscript_path)

    for i, vpu in enumerate(VPU_LIST):
        # Create init flows tasks
        task = Task(f"{task_name}_{vpu}")
        task.add_variable("VPU", vpu)
        if is_local:
            if i > 0:
                prev_vpu = VPU_LIST[i - 1]
                task.add_trigger( f"{task_name}_{prev_vpu} == complete")

        family.add_task(task)

    return family


def create_init_flows_family(
    family_name: str,
    task_name: str,
    trigger: str,
    is_local: bool = False
) -> Family:
    """Create the ecflow family for initializing flows.

    Args:
        family_name (str): Name of the family group.
        task_name (str): Name of the task.
        trigger (str): Trigger that will start this family.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), 'resources', 'compute_init_flows.py'
    )

    family = Family(family_name)
    family.add_trigger(f"{trigger} == complete")
    family.add_variable("PYSCRIPT", pyscript_path)

    for i, vpu in enumerate(VPU_LIST):
        # Create init flows tasks
        task = Task(f"{task_name}_{vpu}")
        task.add_variable("VPU", vpu)
        if is_local:
            if i > 0:
                prev_vpu = VPU_LIST[i - 1]
                task.add_trigger( f"{task_name}_{prev_vpu} == complete")

        family.add_task(task)

    return family


def create_esri_table_family(
    family_name: str,
    task_name: str,
    logs_dir: str,
    nces_exec: str,
    rapid_output: str,
    rp_dir: str,
    trigger: str,
    is_local: bool = False
) -> Family:
    """Create the ecflow family for generating esri tables.

    Args:
        family_name (str): Name of the family group.
        task_name (str): Name of the task.
        logs_dir (str): Path to the logs directory.
        nces_exec (str): Path to the nces executable.
        rapid_output (str): Path to the rapid output directory.
        rp_dir (str): Path to the return periods directory.
        trigger (str): Trigger that will start this family.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), 'resources', 'generate_esri_table.py'
    )

    family = Family(family_name)
    family.add_trigger(f"{trigger} == complete")
    family.add_variable("PYSCRIPT", pyscript_path)
    family.add_variable("LOG_FILE", os.path.join(logs_dir, 'esri_table.log'))
    family.add_variable("NCES_EXEC", nces_exec)

    for i, vpu in enumerate(VPU_LIST):
        # Create init flows tasks
        task = Task(f"{task_name}_{vpu}")
        task.add_variable("OUT_LOCATION", os.path.join(rapid_output, str(vpu)))
        task.add_variable("RETURN_PERIODS_DIR", os.path.join(rp_dir, str(vpu)))
        if is_local:
            if i > 0:
                prev_vpu = VPU_LIST[i - 1]
                task.add_trigger( f"{task_name}_{prev_vpu} == complete")

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
    python_exec = config['python_exec']
    ecflow_home = config['ecflow_home']
    ecflow_bin = config.get('ecflow_bin')
    local_run = config.get('local_run')
    ecflow_entities = config['ecflow_entities']
    ecflow_suite = config['ecflow_entities']['suite']['name']
    ecflow_suite_logs = config['ecflow_entities']['suite']['logs']
    rapid_family_name = config['ecflow_entities']['family'][0]['name']
    init_flows_family_name = config['ecflow_entities']['family'][1]['name']
    esri_table_family_name = config['ecflow_entities']['family'][2]['name']
    nc_to_zarr_family_name = config['ecflow_entities']['family'][3]['name']
    prep_task_name = config['ecflow_entities']['task'][0]['name']
    esri_table_task_name = config['ecflow_entities']['task'][1]['name']
    day_one_forecast_task = config['ecflow_entities']['task'][2]['name']
    rapid_task_name = config['ecflow_entities']['task'][3]['name']
    init_flows_task_name = config['ecflow_entities']['task'][4]['name']
    nc_to_zarr_task_name = config['ecflow_entities']['task'][5]['name']
    rapid_exec = config['rapid_exec']
    rapid_exec_dir = config['rapid_exec_dir']
    rapid_subprocess_dir = config['rapid_subprocess_dir']
    rapid_io = config['rapid_io']
    runoff_dir = config['runoff_dir']
    era_dir = config['era_dir']
    forecast_records_dir = config['forecast_records_dir']
    nces_exec = config['nces_exec']

    # Prepare the directory structure
    prepare_dir_structure(
        python_exec,
        ecflow_home,
        ecflow_entities,
        ecflow_suite_logs
    )

    # Create symlinks for all tasks
    task_family_list = [
        (rapid_task_name, rapid_family_name),
        (init_flows_task_name, init_flows_family_name),
        (esri_table_task_name, esri_table_family_name),
        (nc_to_zarr_task_name, nc_to_zarr_family_name),
    ]
    create_symlinks_for_family_tasks(
        ecflow_home, ecflow_suite, task_family_list
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
        "ECF_BIN": ecflow_bin if local_run else ""
    }
    add_variables(suite, suite_variables)

    # Define 'prep_task'
    prep_task = suite.add_task(prep_task_name)

    # Set variables for 'prep_task'
    prep_task_ps = os.path.join(
        os.path.dirname(__file__), 'resources', 'prep_rapid_forecast.py'
    )
    prep_task_vars = {
        "PYSCRIPT": prep_task_ps,
        "IO_LOCATION": rapid_io,
        "RUNOFF_LOCATION": runoff_dir
    }
    add_variables(prep_task, prep_task_vars)

    # Add the rapid run family to the suite
    rapid_run_family = create_rapid_run_family(
        rapid_family_name,
        rapid_task_name,
        prep_task_name,
        rapid_exec,
        rapid_exec_dir,
        rapid_subprocess_dir,
        is_local=local_run
    )
    suite.add_family(rapid_run_family)

    # Add the init flows family to the suite
    nc_to_zarr_family = create_nc_to_zarr_family(
        nc_to_zarr_family_name,
        nc_to_zarr_task_name,
        rapid_family_name,
        is_local=local_run
    )
    suite.add_family(nc_to_zarr_family)

    # Add the init flows family to the suite
    init_flows_family = create_init_flows_family(
        init_flows_family_name,
        init_flows_task_name,
        nc_to_zarr_family_name,
        is_local=local_run
    )
    suite.add_family(init_flows_family)

    # Add the esri table family to the suite
    esri_table_family = create_esri_table_family(
        esri_table_family_name,
        esri_table_task_name,
        ecflow_suite_logs,
        nces_exec,
        os.path.join(rapid_io, 'output'),
        era_dir,
        init_flows_family_name,
        is_local=local_run
    )
    suite.add_family(esri_table_family)

    # Define 'day_one_forecast_task'
    store_day_one = suite.add_task(day_one_forecast_task)
    store_day_one.add_trigger(f"{rapid_family_name} == complete")

    # Set variables for 'day_one_forecast_task'
    store_day_one_ps = os.path.join(
        os.path.dirname(__file__), 'resources', 'day_one_forecast.py'
    )
    store_day_one_vars = {
        "PYSCRIPT": store_day_one_ps,
        "IO_LOCATION": rapid_io,
        "ERA_LOCATION": era_dir,
        "FORECAST_RECORDS_DIR": forecast_records_dir,
        "LOG_DIR": ecflow_suite_logs
    }
    add_variables(store_day_one, store_day_one_vars)

    # Validate definition job
    validate(defs)

    # Save the ecflow definition job
    output_path = os.path.join(ecflow_home, f"{ecflow_suite}.def")
    print(f"Saving definition to file '{output_path}'")
    defs.save_as_defs(output_path)

    return defs


if __name__ == "__main__":  # pragma: no cover
    create(sys.argv[1])
