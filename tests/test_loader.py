import pytest
from datetime import datetime, timedelta
from gusty.utils import days_ago
from gusty import create_dag


def custom_days_ago():
    return days_ago(17)


def custom_retries(*args):
    retries = 0
    for a in args:
        retries += a
    return retries


@pytest.fixture(scope="session")
def loader_dir():
    return "tests/dags/loader"


@pytest.fixture(scope="session")
def dag(loader_dir):
    dag = create_dag(
        loader_dir,
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
        latest_only=False,
        dag_constructors=[custom_days_ago, custom_retries],
    )
    return dag


@pytest.fixture(scope="session")
def task(dag):
    return dag.__dict__["task_dict"]["loader_task"]


@pytest.fixture(scope="session")
def custom_task(dag):
    return dag.__dict__["task_dict"]["custom_task"]


@pytest.fixture(scope="session")
def sql_task(dag):
    sql_task = dag.task_dict["sql_task"]
    return sql_task


def test_datetime(task):
    start_date = task.__dict__["start_date"]
    assert start_date.date() == datetime(2022, 1, 15).date()


def test_days_ago(task):
    end_date = task.__dict__["end_date"]
    assert end_date == days_ago(4)


def test_timedelta(task):
    retry_delay = task.__dict__["retry_delay"]
    assert retry_delay == timedelta(minutes=20)


def test_retries(task):
    retries = task.__dict__["retries"]
    assert retries == 7


def test_custom_days_ago(custom_task):
    start_date = custom_task.__dict__["start_date"]
    assert start_date == days_ago(17)


def test_custom_retries(custom_task):
    retries = custom_task.__dict__["retries"]
    assert retries == 9


def test_sql_task_start(sql_task):
    start_date = sql_task.__dict__["start_date"]
    assert start_date == days_ago(24)


def test_sql_task_end(sql_task):
    end_date = sql_task.__dict__["end_date"]
    assert end_date == days_ago(17)
