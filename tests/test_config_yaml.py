"""Tests for the YAML deployment-config loader.

These exercise the comfies config layer directly (no ecflow, no server) so they
run anywhere. They guard the switch from the old Python ".cfg" format to plain
YAML: the loader must turn a YAML mapping into the same nested-dict shape
the ``Config`` wrapper navigates, and reject input that is missing or is
not a mapping.
"""

import pytest

from geoglows_ecflow.workflow.comfies.config import (
    Config,
    YAMLConfigFile,
    YAMLConfigPath,
    ConfigNotFoundError,
    ConfigLoadingError,
)

SAMPLE = """\
name: geoglows_test
mode: prod
mars_workers: 3
vpu_list:
  - "101"
  - "102"
jobs:
  root: /tmp/jobs
  destinations:
    default:
      name: localhost
"""


def _write(tmp_path, text, filename="config.yaml"):
    path = tmp_path / filename
    path.write_text(text)
    return str(path)


def test_yaml_path_loads_scalars_and_nested_sections(tmp_path):
    cfg = Config(YAMLConfigPath(_write(tmp_path, SAMPLE)))
    assert cfg.get("name") == "geoglows_test"
    assert cfg.get("mars_workers", type=int) == 3
    assert cfg.get("vpu_list", type=list) == ["101", "102"]
    # dotted path descends into nested mappings
    assert cfg.get("jobs.destinations.default.name") == "localhost"


def test_yaml_section_returns_subtree(tmp_path):
    cfg = Config(YAMLConfigPath(_write(tmp_path, SAMPLE)))
    jobs = cfg.section("jobs")
    assert jobs.get("root") == "/tmp/jobs"


def test_config_file_finds_by_id_on_search_path(tmp_path):
    # YAMLConfigFile resolves a config ID to "<id>.yaml" on its search_path.
    _write(tmp_path, SAMPLE, filename="mysuite.yaml")

    class _LocalConfig(YAMLConfigFile):
        search_path = [str(tmp_path)]

    cfg = Config(_LocalConfig("mysuite"))
    assert cfg.get("name") == "geoglows_test"


def test_missing_path_raises_not_found(tmp_path):
    with pytest.raises(ConfigNotFoundError):
        YAMLConfigPath(str(tmp_path / "does_not_exist.yaml"))


def test_empty_yaml_is_rejected(tmp_path):
    # safe_load("") returns None, which is not a config mapping.
    with pytest.raises(ConfigLoadingError):
        YAMLConfigPath(_write(tmp_path, ""))


def test_non_mapping_yaml_is_rejected(tmp_path):
    with pytest.raises(ConfigLoadingError):
        YAMLConfigPath(_write(tmp_path, "- just\n- a\n- list\n"))


def test_malformed_yaml_is_rejected(tmp_path):
    with pytest.raises(ConfigLoadingError):
        YAMLConfigPath(_write(tmp_path, "key: [unclosed\n"))
