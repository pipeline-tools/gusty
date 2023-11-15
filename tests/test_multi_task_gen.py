from datetime import timedelta

import pytest

from gusty import create_dag
from gusty.parsing import get_dates_range
from gusty.utils import days_ago


###############
## FIXTURES ##
###############


@pytest.fixture(scope="session")
def multi_task_dir():
    return "tests/dags/multi_task_gen"


@pytest.fixture(scope="session")
def dag(multi_task_dir):
    dag = create_dag(
        multi_task_dir,
        description="A dag created without metadata",
        schedule="0 0 * * *",
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


###########
## TESTS ##
###########


def test_multiple_tasks_exist(dag):
    assert "multi_bash_0_1" in dag.task_dict.keys()
    assert "multi_bash_2_3" in dag.task_dict.keys()
    assert "multi_bash_4_5" in dag.task_dict.keys()
    assert "multi_bash_10_11" not in dag.task_dict.keys()


def test_multiple_tasks_exist_date(dag):
    for cur_date in get_dates_range(days_ago(4), days_ago(1)):
        assert f"multi_bash_date_{cur_date}" in dag.task_dict.keys()
