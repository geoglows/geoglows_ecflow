import os
import shutil
from yaml import safe_load
from ecflow import Defs, Suite, Family, Task
from geoglows_ecflow.resources.constants import VPU_LIST


def load_config(config_path: str) -> dict:
    """Load the configuration file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        dict: dictionary containing the configuration parameters.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
    """
    try:
        with open(config_path, "r") as f:
            config = safe_load(f)

        return config
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Error loading configuration file: {e}")


def prepare_dir_structure(
    python_exec: str, workspace: str, entities: dict, suite_logs: str
) -> None:
    """Create the directory structure for the ecflow job.

    Args:
        python_exec (str): Path to the python executable.
        workspace (str): Path to the job workspace.
        entities (dict): Dictionary containing the entities of the ecflow job.
            See config.yml.
        suite_logs (str): Path to the suite logs directory.
    Raises:
        OSError: If the directory structure cannot be created.
    """
    # Create files and directories
    try:
        for type, ent in entities.items():
            if type == "suite":
                suite_path = os.path.join(workspace, ent["name"])
                if not os.path.exists(suite_path):
                    os.makedirs(suite_path)
                if not os.path.exists(suite_logs):
                    os.makedirs(suite_logs)

            elif type == "family":
                for fam in ent:
                    family_path = os.path.join(
                        workspace, fam["suite"], fam["name"]
                    )
                    if not os.path.exists(family_path):
                        os.makedirs(family_path)

            elif type == "task":
                for task in ent:
                    task_path = os.path.join(
                        workspace, task["suite"], f"{task['name']}.ecf"
                    )

                    var_list = " ".join([f"%{var}%" for var in task["variables"]])

                    if not os.path.exists(task_path):
                        with open(task_path, "w") as f:
                            f.write("%include <head.h>\n")
                            f.write(f"{python_exec} {var_list}\n")
                            f.write("%include <tail.h>\n")

        # Copy head.h and tail.h files
        shutil.copyfile(
            os.path.join(os.path.dirname(__file__), "resources", "head.h"),
            os.path.join(workspace, "head.h"),
        )
        shutil.copyfile(
            os.path.join(os.path.dirname(__file__), "resources", "tail.h"),
            os.path.join(workspace, "tail.h"),
        )
    except OSError as e:
        raise OSError(f"Error creating files/directory: {e}")


def create_symlinks_for_ensemble_tasks(
    workspace: str, task: str, family: str, suite: str
) -> None:
    """Creates a symlink of 'rapid_forecast_task.ecf' for each vpu and ensemble
        member or a symlink of 'init_flows_task.ecf' for each vpu.

    Args:
        workspace (str): Path to the job workspace.
        task (str): Name of the task.
        family (str): Family group for the task.
        suite (str): Suite of the task.
    """
    for vpu in VPU_LIST:
        if task == "init_flows_task":
            src = os.path.join(workspace, suite, f"{task}.ecf")
            dest = os.path.join(workspace, suite, family, f"{task}_{vpu}.ecf")
            if not os.path.exists(dest):
                os.symlink(src, dest)
        else:
            for i in reversed(range(1, 53)):
                src = os.path.join(workspace, suite, f"{task}.ecf")
                dest = os.path.join(
                    workspace, suite, family, f"{task}_{vpu}_{i}.ecf"
                )
                if not os.path.exists(dest):
                    os.symlink(src, dest)


def add_variables(entity: Suite | Family | Task, vars: dict[str, str]) -> None:
    """Add variables to the ecflow entity.

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
