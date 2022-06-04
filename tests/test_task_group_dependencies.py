import pytest
from gusty import create_dag
from datetime import timedelta
from gusty.utils import days_ago

###############
## FIXTURES ##
###############


@pytest.fixture(scope="session")
def task_group_dependencies_dir():
    return "tests/dags/task_group_dependencies"


@pytest.fixture(scope="session")
def dag(task_group_dependencies_dir):
    dag = create_dag(
        task_group_dependencies_dir,
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
        leaf_tasks=["simple_leaf"],
    )
    return dag


###########
## TESTS ##
###########


def test_task_group_dependencies(dag):
    """
    This test is to ensure that, when root tasks are created (e.g. latest_only),
    task groups which depend on other tasks groups are not considered valid dependency
    objects for root tasks.
    """
    second_task_group = dag.__dict__["_task_group"].__dict__["children"][
        "second_task_group"
    ]
    assert "latest_only" not in second_task_group.upstream_task_ids
