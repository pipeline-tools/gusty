import pytest
from gusty import create_dag

###############
## FIXTURES ##
###############


@pytest.fixture(scope="session")
def with_metadata_dir():
    return "tests/dags/with_metadata"


@pytest.fixture(scope="session")
def dag(with_metadata_dir):
    dag = create_dag(
        with_metadata_dir,
        task_group_defaults={"prefix_group_id": True},
        wait_for_defaults={"poke_interval": 12},
    )
    return dag


###########
## TESTS ##
###########


def test_latest_only_false(dag):
    assert "latest_only" not in dag.roots


def test_prefixes(dag):
    assert "prefixes.prefixes_check" in dag.task_dict.keys()


def test_suffixes(dag):
    assert "suffixes.check_suffixes" in dag.task_dict.keys()


def test_noffixes(dag):
    assert "plain_name" in dag.task_dict.keys()


def test_deeper(dag):
    # even though this is deep, it drops all tags when prefix is dropped
    # even though it still lives in the taskgroup deeper.first...
    # maybe an airflow bug?
    assert "first" in dag.task_dict.keys()
    assert "deeper.second.second_second" in dag.task_dict.keys()
