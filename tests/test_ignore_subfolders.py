import pytest
import os
from airflow import DAG
from datetime import datetime, timedelta
from gusty.utils import days_ago
from gusty import create_dag


# This module is intended to test the default behavior of gusty
# Given a directory of tasks without any metadata located in those directories
# A sort of baseline


##############
## FIXTURES ##
##############


@pytest.fixture(scope="session")
def no_metadata_dir():
    return "tests/dags/ignore_subfolders"


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
        ignore_subfolders=True,
    )
    return dag


###########
## TESTS ##
###########


def test_ignore_subfolders(dag):
    assert "ignore_this_task" not in dag.task_dict.keys()


def test_ignore_skiplevel(dag):
    assert "ignore_this_too" not in dag.task_dict.keys()
