import pytest
from gusty import create_dag
from datetime import timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator

###############
## FIXTURES ##
###############


@pytest.fixture(scope="session")
def external_deps_dir():
    return "tests/dags/external_deps"


@pytest.fixture(scope="session")
def custom_sensor_class():
    class CustomExternalTaskSensor(ExternalTaskSensor):
        is_custom_sensor = True

    return CustomExternalTaskSensor


@pytest.fixture(scope="session")
def dag(external_deps_dir, custom_sensor_class):
    dag = create_dag(
        external_deps_dir,
        default_args={"email": "default@gusty.com", "retries": 5},
        task_group_defaults={"prefix_group_id": True},
        wait_for_defaults={"poke_interval": 12},
        wait_for_class=custom_sensor_class,
    )
    return dag


@pytest.fixture(scope="session")
def empty_dag(external_deps_dir):
    dag = create_dag(
        external_deps_dir,
        default_args={"email": "default@gusty.com", "retries": 5},
        task_group_defaults={"prefix_group_id": True},
        wait_for_defaults={"poke_interval": 12},
        wait_for_class=EmptyOperator,
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


def test_custom_sensor(dag, custom_sensor_class):
    wait_for_task = dag.task_dict["wait_for_some_task"]
    assert isinstance(wait_for_task, custom_sensor_class)
    assert wait_for_task.is_custom_sensor


def test_empty_dag(empty_dag):
    wait_for_task = empty_dag.task_dict["wait_for_some_task"]
    assert isinstance(wait_for_task, EmptyOperator)
