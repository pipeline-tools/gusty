import pytest
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from gusty import create_dag
from gusty.parsing.loaders import GustyYAMLLoader


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
    )
    return dag


@pytest.fixture(scope="session")
def task(dag):
    return dag.__dict__["task_dict"]["loader_task"]


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
