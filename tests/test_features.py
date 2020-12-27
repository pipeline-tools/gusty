import pytest
import os
from airflow import DAG
from airflow.models import BaseOperator
from gusty import GustyDAG

##############
## FIXTURES ##
##############

# Where are the specs for the DAG we are building?
@pytest.fixture
def dag_dir():
    return "examples/breakfast_dag"


@pytest.fixture
def dag_files(dag_dir):
    dag_files = [
        file
        for file in os.listdir(dag_dir)
        if os.path.isfile(os.path.join(dag_dir, file)) and file != "METADATA.yml"
    ]
    return dag_files


@pytest.fixture
def dag(dag_dir):
    dag = GustyDAG(dag_dir)
    return dag


@pytest.fixture
def dag_tasks(dag):
    dag_tasks = dag.task_dict
    return dag_tasks


# This task should include:
#   - Explicit dependencies
#   - Explicit external dependencies (single task and an all task)
#   - An override of the DAG's default number of retries
@pytest.fixture
def featured_task(dag_tasks):
    return dag_tasks["enjoy_breakfast"]


###############
## DAG TESTS ##
###############

# Did we make an Airflow DAG?
def test_is_dag(dag):
    assert isinstance(dag, DAG)


# Is the DAG named after its directory?
def test_dag_name(dag, dag_dir):
    assert dag._dag_id == os.path.basename(dag_dir)


################
## TASK TESTS ##
################

# Do we create operators?
def test_tasks_are_operators(featured_task):
    assert isinstance(featured_task, BaseOperator)


# Did we create a task for every file in the directory? (Besides METADATA.yml)
# (Also accounting for latest_only option in gusty and any wait_for tasks)
def test_dag_tasks_exist(dag_tasks, dag_files):
    dag_dir_files = dag_files
    task_files = len(dag_dir_files)
    expected_tasks = len(
        [
            task
            for task in dag_tasks.keys()
            if not task.startswith("wait_for_") and task != "latest_only"
        ]
    )
    assert task_files == expected_tasks


# Are created tasks named after their file names?
def test_task_names(dag_files, dag_tasks):
    files_to_check = [
        os.path.splitext(file)[0]
        for file in dag_files
    ]
    tasks_created = [name in dag_tasks.keys() for name in files_to_check]
    assert all(tasks_created)


# Does gusty override a DAG's default args when one is passed to a task?
def test_task_overrides_dag_defaults(dag, featured_task):
    assert featured_task.retries > dag.default_args["retries"]


# Are local depencies set?
def test_task_local_dependencies(featured_task):
    assert (
        len(
            [
                id
                for id in featured_task._upstream_task_ids
                if not id.startswith("wait_for_")
            ]
        )
        > 0
    )


# Is a wait_for task created for a single task in another DAG?
def test_task_single_external_dependency(featured_task):
    assert (
        len(
            [
                id
                for id in featured_task._upstream_task_ids
                if id.startswith("wait_for_") and not id.startswith("wait_for_DAG")
            ]
        )
        > 0
    )


# Is a wait_for_DAG task created for an entire external DAG dependency?
def test_task_full_dag_external_dependency(featured_task):
    assert (
        len(
            [
                id
                for id in featured_task._upstream_task_ids
                if id.startswith("wait_for_DAG")
            ]
        )
        > 0
    )
