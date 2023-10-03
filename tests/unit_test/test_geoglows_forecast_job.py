

import os
import pytest
from geoglows_ecflow.geoglows_forecast_job import create_rapid_run_family, create
from ecflow import Expression, Family, Defs


@pytest.mark.parametrize(
    "is_local, trigger_expression",
    [(True, Expression(f"ens_member_2 == complete")), (False, None)]
)
def test_create_ensemble_family(is_local, trigger_expression):
    family_name = "test_family"
    ensemble_member_task = "ens_member"
    rapid_exec = "/path/to/rapid/executable"
    rapid_exec_dir = "/path/to/rapid/executable/directory"
    rapid_subprocess_dir = "/path/to/rapid/subprocess/directory"

    family = create_rapid_run_family(
        family_name,
        ensemble_member_task,
        rapid_exec,
        rapid_exec_dir,
        rapid_subprocess_dir,
        is_local=is_local
    )

    assert isinstance(family, Family)
    assert family.name() == family_name
    assert len(family) == 52

    t52 = family.find_task("ens_member_52")
    assert t52 is not None
    assert t52.find_variable("JOB_INDEX").value() == "0"

    t1 = family.find_task("ens_member_1")
    assert t1 is not None
    assert t1.find_variable("JOB_INDEX").value() == "51"
    assert t1.get_trigger() == trigger_expression


def test_create(capsys, mocker):
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "files",
        "mock_config_test.yml"
    )
    expected_output = Defs(os.path.join(os.path.dirname(
        os.path.dirname(__file__)), "files", "mock_defs_novar.def"
    ))

    mock_save_defs = mocker.patch(
        "geoglows_ecflow.geoglows_forecast_job.Defs.save_as_defs"
    )
    mocker.patch("geoglows_ecflow.geoglows_forecast_job.validate")
    mocker.patch("geoglows_ecflow.geoglows_forecast_job.add_variables")
    mocker.patch(
        "geoglows_ecflow.geoglows_forecast_job.create_symlinks_for_ensemble_tasks"  # noqa: E501
    )
    mocker.patch("geoglows_ecflow.geoglows_forecast_job.prepare_dir_structure")
    mocker.patch("os.makedirs")
    mocker.patch("os.path.exists", return_value=False)
    with capsys.disabled():
        defs = create(config_path)

    suite = defs.find_suite("geoglows_forecast")
    assert suite is not None
    assert suite.find_task("prep_task") is not None
    assert suite.find_family("ensemble_family") is not None
    assert suite.find_task("plain_table_task") is not None
    assert suite.find_task("day_one_forecast_task") is not None

    mock_save_defs.assert_called_with(
        "/path/to/ecflow_home/geoglows_forecast.def"
    )

    assert defs == expected_output
