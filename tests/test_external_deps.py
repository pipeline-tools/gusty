import pytest
from gusty import create_dag
from datetime import timedelta

###############
## FIXTURES ##
###############


@pytest.fixture(scope="session")
def external_deps_dir():
    return "tests/dags/external_deps"


@pytest.fixture(scope="session")
def dag(external_deps_dir):
    dag = create_dag(
        external_deps_dir,
        default_args={"email": "default@gusty.com", "retries": 5},
        task_group_defaults={"prefix_group_id": True},
        wait_for_defaults={"poke_interval": 12},
    )
    return dag


###########
## TESTS ##
###########


def test_dag_level_ext_dep(dag):
    wait_for_task = dag.task_dict["wait_for_DAG_top_level_external"]
    assert wait_for_task.__dict__["execution_delta"] == timedelta(minutes=88)


def test_task_group_level_ext_dep(dag):
    wait_for_task = dag.task_dict["wait_for_some_task"]
    assert wait_for_task.__dict__["execution_delta"] == timedelta(minutes=89)


def test_task_level_ext_dep(dag):
    wait_for_task = dag.task_dict["wait_for_another_task_1"]
    assert wait_for_task.__dict__["execution_delta"] == timedelta(minutes=95)
    assert wait_for_task.__dict__["retries"] == 95
