import pytest
import os
from airflow import DAG
from airflow.models import BaseOperator
from datetime import datetime, timedelta
from gusty.utils import days_ago
from gusty.building import create_schematic
from gusty.parsing import default_parsers
from gusty import create_dag


# This module is intended to test the default behavior of gusty
# Given a directory of tasks without any metadata located in those directories
# A sort of baseline


##############
## FIXTURES ##
##############


@pytest.fixture(scope="session")
def no_metadata_dir():
    return "tests/dags/no_metadata"


@pytest.fixture(scope="session")
def dag_files(no_metadata_dir):
    schematic = create_schematic(no_metadata_dir)
    dag_file_list = [level["spec_paths"] for level_id, level in schematic.items()]
    dag_files = [item for sublist in dag_file_list for item in sublist]
    return dag_files


@pytest.fixture(scope="session")
def dag(no_metadata_dir):
    dag = create_dag(
        no_metadata_dir,
        description="A dag created without metadata",
        schedule_interval="0 0 * * *",
        default_args={
            "owner": "gusty",
            "depends_on_past": False,
            "start_date": days_ago(1),
            "email": "gusty@gusty.com",
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 3,
            "retry_delay": timedelta(minutes=5),
        },
        external_dependencies=[{"external_dag": "a_root_level_external"}],
    )
    return dag


###########
## TESTS ##
###########


def test_dag_is_dag(dag):
    assert isinstance(dag, DAG)


def test_dag_named_after_dir(dag, no_metadata_dir):
    assert dag.dag_id == os.path.basename(no_metadata_dir)


def test_latest_only_root(dag):
    assert len(dag.roots) == 1
    assert dag.roots[0].task_id == "latest_only"


def test_task_named_after_file(dag):
    top_level_task = dag.task_dict["top_level_task"]
    assert top_level_task is not None


def test_default_no_task_group_prefixes(dag):
    assert "task_overrides_retries" in dag.task_dict.keys()


def test_ipynb_task_exists(dag):
    assert "dependent_jupyter_task" in dag.task_dict.keys()


def test_task_overrides_defaults(dag):
    override_task = dag.task_dict["task_overrides_retries"]
    assert override_task.__dict__["retries"] > dag.default_args["retries"]


def test_top_level_depends_on_second_level(dag):
    top_level_task = dag.task_dict["top_level_task"]
    top_level_task_dependencies = top_level_task.__dict__["upstream_task_ids"]
    assert "dependable_task" in top_level_task_dependencies


def test_external_dependencies(dag):
    dependable_task = dag.task_dict["dependable_task"]
    dependable_task_dependencies = dependable_task.__dict__["upstream_task_ids"]
    assert "wait_for_DAG_a_whole_dag" in dependable_task_dependencies
    assert "wait_for_external_task" in dependable_task_dependencies
    assert "wait_for_external_task_2" in dependable_task_dependencies


def test_external_dependencies_dict_tasks(dag):
    ext_dep_dict_spec_task = dag.task_dict["task_overrides_retries"]
    ext_dep_dict_spec_task_dependencies = ext_dep_dict_spec_task.__dict__[
        "upstream_task_ids"
    ]
    assert "wait_for_DAG_a_whole_dag" in ext_dep_dict_spec_task_dependencies
    assert "wait_for_another_task_1" in ext_dep_dict_spec_task_dependencies
    assert "wait_for_another_task_2" in ext_dep_dict_spec_task_dependencies


def test_tasks_created(dag, dag_files):
    def replace_extension(file):
        final = None
        for extension in default_parsers.keys():
            if file.endswith(extension):
                final = file.replace(extension, "")
        return final

    expected_tasks = [*map(os.path.basename, dag_files)]
    expected_tasks = [*map(replace_extension, expected_tasks)]
    tasks_present = [task in dag.task_dict.keys() for task in expected_tasks]
    tasks_are_operators = [
        isinstance(dag.task_dict[task], BaseOperator) for task in expected_tasks
    ]
    assert all(expected_tasks)
    assert all(tasks_are_operators)


def test_args_passed(dag):
    top_level_task = dag.task_dict["top_level_task"]
    assert top_level_task.__dict__["bash_command"] == "echo hello"


def test_sensor_support(dag):
    assert dag.task_dict["sensor_task"].__dict__["delta"] == timedelta(seconds=2)


def test_root_external_dependencies_accepted(dag):
    assert (
        "wait_for_a_root_level_external"
        in dag.task_dict["latest_only"].__dict__["downstream_task_ids"]
    )


def test_root_external_dependencies_latest_only_order(dag):
    # Bad test name, but bascially nothing should be upstream of latest only,
    # And only the DAG-level external dependency is after latest_only
    latest_only = dag.task_dict["latest_only"].__dict__
    latest_only_downstream_tasks = latest_only["downstream_task_ids"]
    latest_only_upstream_tasks = latest_only["upstream_task_ids"]
    assert (
        len(latest_only_upstream_tasks) == 0 and len(latest_only_downstream_tasks) == 1
    )
