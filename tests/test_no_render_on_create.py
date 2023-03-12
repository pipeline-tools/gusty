import pytest
from datetime import timedelta
from gusty.utils import days_ago
from gusty import create_dag


# This module is intended to test the default behavior of gusty
# Given a directory of tasks without any metadata located in those directories
# A sort of baseline


##############
## FIXTURES ##
##############


@pytest.fixture(scope="session")
def no_render_on_create_dir():
    return "tests/dags/no_render_on_create"


@pytest.fixture(scope="session")
def dag(no_render_on_create_dir):
    dag = create_dag(
        no_render_on_create_dir,
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
    )
    return dag


###########
## TESTS ##
###########


def test_no_render(dag):
    bash_command = dag.task_dict["no_render"].bash_command
    assert bash_command == "echo {{ti.task_id}}"
