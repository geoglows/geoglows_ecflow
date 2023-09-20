import os
from yaml import safe_load
from ecflow import Defs, Suite, Family, Task


def load_config() -> dict:
    """Load the configuration file.

    Returns:
        dict: dictionary containing the configuration parameters.
    """
    with open(os.path.join(os.path.dirname(__file__), 'config.yml'), 'r') as f:
        config = safe_load(f)

    return config


def prepare_dir_structure(
    python_exec: str,
    workspace: str,
    entities: dict
) -> None:
    """Create the directory structure for the ecflow job.

    Args:
        python_exec (str): Path to the python executable.
        workspace (str): Path to the job workspace.
        entities (dict): Dictionary containing the entities of the ecflow job.
            See config.yml.
    """
    for type, ent in entities.items():
        if type == 'suite':
            suite_path = os.path.join(workspace, ent['name'])
            suite_logs_path = os.path.join(workspace, ent['name'], ent['logs'])
            if not os.path.exists(suite_path):
                os.makedirs(suite_path)
            if not os.path.exists(suite_logs_path):
                os.makedirs(suite_logs_path)

        elif type == 'family':
            family_path = os.path.join(workspace, ent['suite'], ent['name'])
            if not os.path.exists(family_path):
                os.makedirs(family_path)

        elif type == 'task':
            for task in ent:
                task_path = os.path.join(
                    workspace, task['suite'], f"{task['name']}.ecf"
                )
                var_list = " ".join([f'%{var}%' for var in task['variables']])

                if not os.path.exists(task_path):
                    with open(task_path, 'w') as f:
                        f.write('%include <head.h>\n')
                        f.write(f"{python_exec} {var_list}\n")
                        f.write('%include <tail.h>\n')


def create_symlinks_for_ensemble_tasks(
    workspace: str,
    task: str,
    family: str,
    suite: str
) -> None:
    """Creates a copy of 'ens_member.ecf' for each ensemble member.

    Args:
        workspace (str): Path to the job workspace.
        task (str): Name of the task.
        family (str): Family group for the task.
        suite (str): Suite of the task.
    """
    for i in reversed(range(1, 53)):
        src = os.path.join(workspace, suite, f'{task}.ecf')
        dest = os.path.join(workspace, suite, family,
                            f'{task}_{i}.ecf')
        if not os.path.exists(dest):
            os.symlink(src, dest)


def create_ensemble_family(
    workspace: str,
    family: str,
    pyscript: str,
    rapid_exec: str,
    rapid_exec_dir: str,
    rapid_subprocess_dir: str,
    is_local: bool = False
) -> Family:
    """_summary_

    Args:
        workspace (str): Path to the job workspace.
        family (str): Name of the family group.
        pyscript (str): Python script to associate with family.
        rapid_exec (str): Path to the rapid executable.
        rapid_exec_dir (str): Path to the rapid executable directory.
        rapid_subprocess_dir (str): Path to the rapid subprocess directory.
        is_local (bool, optional): True if the job is run locally.

    Returns:
        Family: ecflow family object.
    """
    pyscript_path = os.path.join(workspace, pyscript)

    ensemble_family = Family(family)
    ensemble_family.add_trigger("prep_task == complete")
    ensemble_family.add_variable("PYSCRIPT", pyscript_path)
    ensemble_family.add_variable("RAPID_EXEC", rapid_exec)
    ensemble_family.add_variable("EXEC_DIR", rapid_exec_dir)
    ensemble_family.add_variable("SUBPROCESS_DIR", rapid_subprocess_dir)

    # Create the high resolution ensemble task
    ensemble_family += [Task(f"ens_member_52").add_variable("JOB_INDEX", 0)]

    # Create the ensemble tasks
    for i in reversed(range(1, 52)):
        ens_task = Task(f"ens_member_{i}")
        ens_task.add_variable("JOB_INDEX", 52 - i)
        if is_local:
            ens_task.add_trigger(f"ens_member_{i + 1} == complete")
        ensemble_family += [ens_task]

    return ensemble_family


def add_variables(
    entity: Suite | Family | Task,
    vars: dict[str, str]
) -> None:
    """_summary_

    Args:
        entity (Suite | Family | Task): Entity to add variables to.
        vars (dict[str, str]): Variables to add to the entity.
    """
    for key, value in vars.items():
        entity.add_variable(key, value)


def validate(defs: Defs) -> None:
    """Print the ecflow job and check for errors

    Args:
        defs (Defs): Job definition to validate.
    """
    print(defs)
    print("Checking trigger expressions")

    check = defs.check()
    assert len(check) == 0, check

    print("Checking job creation: .ecf -> .job0")
    print(defs.check_job_creation())


def main() -> None:
    """Create the ecflow job definition file and its file structure."""
    # Load the configuration file
    config = load_config()
    python_exec = config['python_exec']
    ecflow_home = config['ecflow_home']
    ecflow_bin = config.get('ecflow_bin')
    local_run = config['local_run']
    ecflow_entities = config['ecflow_entities']
    ecflow_suite = config['ecflow_entities']['suite']['name']
    ecflow_suite_logs = config['ecflow_entities']['suite']['logs']
    ensemble_family = config['ecflow_entities']['family']['name']
    ensemble_family_ps = config['ecflow_entities']['family']['pyscript']
    prep_task = config['ecflow_entities']['task'][0]['name']
    plain_table_task = config['ecflow_entities']['task'][1]['name']
    day_one_forecast_task = config['ecflow_entities']['task'][2]['name']
    ensemble_member_task = config['ecflow_entities']['task'][3]['name']
    prep_task_ps = config['ecflow_entities']['task'][0]['pyscript']
    plain_table_task_ps = config['ecflow_entities']['task'][1]['pyscript']
    day_one_forecast_task_ps = config['ecflow_entities']['task'][2]['pyscript']
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
        ecflow_entities
    )

    # Create symbolic links to 'ens_member.ecf' file for each ensemble member
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
    prep_task_vars = {
        "PYSCRIPT": os.path.join(ecflow_home, prep_task_ps),
        "IO_LOCATION": rapid_io,
        "RUNOFF_LOCATION": runoff_dir
    }
    add_variables(prep_task, prep_task_vars)

    # Add the ensemble family to the suite
    suite += create_ensemble_family(
        ecflow_home,
        ensemble_family,
        ensemble_family_ps,
        rapid_exec,
        rapid_exec_dir,
        rapid_subprocess_dir,
        is_local=local_run
    )

    # Define 'plain_table_task'
    plain_table_task = suite.add_task(plain_table_task)
    plain_table_task.add_trigger(f"{ensemble_family} == complete")

    # Set variables for 'plain_table_task'
    plain_table_vars = {
        "PYSCRIPT": os.path.join(ecflow_home, plain_table_task_ps),
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
    store_day_one_vars = {
        "PYSCRIPT": os.path.join(ecflow_home, day_one_forecast_task_ps),
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


if __name__ == "__main__":
    main()
