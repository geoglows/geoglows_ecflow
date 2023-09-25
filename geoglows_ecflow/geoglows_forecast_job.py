import os
import sys
from ecflow import Defs, Family, Task
from geoglows_ecflow.utils import (load_config, prepare_dir_structure,
                                   create_symlinks_for_ensemble_tasks,
                                   add_variables, validate)


def create_ensemble_family(
    family: str,
    task: str,
    rapid_exec: str,
    rapid_exec_dir: str,
    rapid_subprocess_dir: str,
    is_local: bool = False
) -> Family:
    """_summary_

    Args:
        family (str): Name of the family group.
        task (str): Name of the task for each ensemble.
        rapid_exec (str): Path to the rapid executable.
        rapid_exec_dir (str): Path to the rapid executable directory.
        rapid_subprocess_dir (str): Path to the rapid subprocess directory.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(
        os.path.dirname(__file__), 'resources', 'run_ecflow.py'
    )

    ensemble_family = Family(family)
    ensemble_family.add_trigger("prep_task == complete")
    ensemble_family.add_variable("PYSCRIPT", pyscript_path)
    ensemble_family.add_variable("RAPID_EXEC", rapid_exec)
    ensemble_family.add_variable("EXEC_DIR", rapid_exec_dir)
    ensemble_family.add_variable("SUBPROCESS_DIR", rapid_subprocess_dir)

    # Create the high resolution ensemble task
    ensemble_family += [Task(f"{task}_52").add_variable("JOB_INDEX", 0)]

    # Create the ensemble tasks
    for i in reversed(range(1, 52)):
        ens_task = Task(f"{task}_{i}")
        ens_task.add_variable("JOB_INDEX", 52 - i)
        if is_local:
            ens_task.add_trigger(f"{task}_{i + 1} == complete")
        ensemble_family += [ens_task]

    return ensemble_family


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
    local_run = config['local_run']
    ecflow_entities = config['ecflow_entities']
    ecflow_suite = config['ecflow_entities']['suite']['name']
    ecflow_suite_logs = config['ecflow_entities']['suite']['logs']
    ensemble_family = config['ecflow_entities']['family']['name']
    prep_task = config['ecflow_entities']['task'][0]['name']
    plain_table_task = config['ecflow_entities']['task'][1]['name']
    day_one_forecast_task = config['ecflow_entities']['task'][2]['name']
    ensemble_member_task = config['ecflow_entities']['task'][3]['name']
    rapid_exec = config['rapid_exec']
    rapid_exec_dir = config['rapid_exec_dir']
    rapid_subprocess_dir = config['rapid_subprocess_dir']
    rapid_io = config['rapid_io']
    runoff_dir = config['runoff_dir']
    era_type = config['era_type']
    era_dir = config['era_dir']
    nces_exec = config['nces_exec']

    # Prepare the directory structure
    prepare_dir_structure(
        python_exec,
        ecflow_home,
        ecflow_entities,
        ecflow_suite_logs
    )

    # Create symbolic links to '.ecf' file for each ensemble member task
    create_symlinks_for_ensemble_tasks(
        ecflow_home,
        ensemble_member_task,
        ensemble_family,
        ecflow_suite
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
    prep_task = suite.add_task(prep_task)

    # Set variables for 'prep_task'
    prep_task_ps = os.path.join(
        os.path.dirname(__file__), 'resources', 'iprep_ecf.py'
    )
    prep_task_vars = {
        "PYSCRIPT": prep_task_ps,
        "IO_LOCATION": rapid_io,
        "RUNOFF_LOCATION": runoff_dir
    }
    add_variables(prep_task, prep_task_vars)

    # Add the ensemble family to the suite
    suite += create_ensemble_family(
        ensemble_family,
        ensemble_member_task,
        rapid_exec,
        rapid_exec_dir,
        rapid_subprocess_dir,
        is_local=local_run
    )

    # Define 'plain_table_task'
    plain_table_task = suite.add_task(plain_table_task)
    plain_table_task.add_trigger(f"{ensemble_family} == complete")

    # Set variables for 'plain_table_task'
    plain_table_ps = os.path.join(
        os.path.dirname(__file__), 'resources', 'spt_extract_plain_table.py'
    )
    plain_table_vars = {
        "PYSCRIPT": plain_table_ps,
        "OUT_LOCATION": f"{rapid_io}/output",
        "LOG_FILE": os.path.join(ecflow_suite_logs, 'plain_table.log'),
        "NCES_EXEC": nces_exec,
        "ERA_TYPE": era_type
    }
    add_variables(plain_table_task, plain_table_vars)

    # Define 'day_one_forecast_task'
    store_day_one = suite.add_task(day_one_forecast_task)
    store_day_one.add_trigger(f"{ensemble_family} == complete")

    # Set variables for 'day_one_forecast_task'
    store_day_one_ps = os.path.join(
        os.path.dirname(__file__), 'resources', 'day_one_forecast.py'
    )
    store_day_one_vars = {
        "PYSCRIPT": store_day_one_ps,
        "IO_LOCATION": rapid_io,
        "ERA_LOCATION": era_dir,
        "FORECAST_RECORDS_DIR": rapid_io,
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
