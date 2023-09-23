

import os
import pytest
from geoglows_ecflow.utils import (load_config, prepare_dir_structure,
                                   create_symlinks_for_ensemble_tasks)
from ecflow import Expression, Family, Defs


def test_load_config():
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "files", "mock_config.yml"
    )
    config = load_config(config_path)

    assert isinstance(config, dict)
    assert config['ecflow_home'] == "/path/to/ecflow_home"

    # Test loading a non-existent configuration file
    with pytest.raises(FileNotFoundError):
        load_config('tests/nonexistent_config.yml')


def test_prepare_dir_structure(tmpdir):
    # Define test data
    python_exec = '/usr/bin/python'
    workspace = str(tmpdir.mkdir('workspace'))
    entities = {
        'suite': {
            'name': 'my_suite',
            'logs': 'logs'
        },
        'family': {
            'suite': 'my_suite',
            'name': 'my_family'
        },
        'task': [
            {
                'suite': 'my_suite',
                'name': 'my_task',
                'variables': ['VAR1', 'VAR2']
            }
        ]
    }
    suite_logs = os.path.join(workspace, 'my_suite', 'logs')

    # Call the function
    prepare_dir_structure(python_exec, workspace, entities, suite_logs)

    # Check that the directory structure was created correctly
    assert os.path.exists(suite_logs)
    assert os.path.exists(os.path.join(workspace, 'my_suite'))
    assert os.path.exists(os.path.join(workspace, 'my_suite', 'my_family'))
    assert os.path.exists(os.path.join(workspace, 'my_suite', 'my_task.ecf'))

    # Check that the task file was created correctly
    with open(os.path.join(workspace, 'my_suite', 'my_task.ecf'), 'r') as f:
        contents = f.read()
        var1 = entities['task'][0]['variables'][0]
        var2 = entities['task'][0]['variables'][1]
        assert '%include <head.h>\n' in contents
        assert f"{python_exec} %{var1}% %{var2}%\n" in contents
        assert '%include <tail.h>\n' in contents

    # Check that the head.h and tail.h files were copied correctly
    assert os.path.exists(os.path.join(workspace, 'head.h'))
    assert os.path.exists(os.path.join(workspace, 'tail.h'))

    # Check that the function raises an OSError
    suite_logs = '/path/to/nonexistent/logs'
    with pytest.raises(OSError):
        prepare_dir_structure(python_exec, workspace, entities, suite_logs)


@pytest.mark.parametrize('existing_symlink,existing_file', [
    (False, False),
    (True, False),
    (False, True)
])
def test_create_symlinks_for_ensemble_tasks(tmpdir, existing_symlink, existing_file):
    # Define test data
    workspace = str(tmpdir.mkdir('workspace'))
    suite = 'my_suite'
    family = 'my_family'
    task = 'my_task'
    task_file = os.path.join(workspace, suite, f'{task}.ecf')
    family_dir = os.path.join(workspace, suite, family)
    os.makedirs(family_dir)
    for i in range(1, 53):
        os.makedirs(os.path.join(workspace, suite, f'ens{i}'))

    # Create the task file
    with open(task_file, 'w') as f:
        f.write('test')

    # Create an existing symlink or file if specified
    if existing_symlink:
        os.symlink(task_file, os.path.join(family_dir, f'{task}_1.ecf'))
    if existing_file:
        with open(os.path.join(family_dir, f'{task}_1.ecf'), 'w') as f:
            f.write('test')

    # Call the function and check the result
    if existing_file:
        with pytest.raises(OSError):
            create_symlinks_for_ensemble_tasks(workspace, task, family, suite)
    else:
        create_symlinks_for_ensemble_tasks(workspace, task, family, suite)
        for i in range(1, 53):
            symlink = os.path.join(family_dir, f'{task}_{i}.ecf')
            assert os.path.exists(symlink)
            assert os.path.islink(symlink)
            assert os.readlink(symlink) == task_file

    # Remove the temporary directory
    tmpdir.remove()
