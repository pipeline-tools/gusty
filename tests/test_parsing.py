import pytest
from gusty.parsing import read_yaml_spec


def test_read_yaml_spec():
    yaml_spec = read_yaml_spec("tests/dags/no_metadata/top_level_task.yml")
    assert yaml_spec["task_id"] == "top_level_task"
    assert yaml_spec["file_path"] == "tests/dags/no_metadata/top_level_task.yml"
    assert "operator" in yaml_spec.keys()
    assert "bash_command" in yaml_spec.keys()
