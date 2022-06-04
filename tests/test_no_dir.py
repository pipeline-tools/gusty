import pytest
from gusty import create_dag
from gusty.errors import NonexistentDagDirError

##############
## FIXTURES ##
##############


@pytest.fixture(scope="session")
def nonexistent_dir():
    return "tests/dagz/nonexistent"


###########
## TESTS ##
###########


def test_missing_dag_error(nonexistent_dir):
    with pytest.raises(NonexistentDagDirError) as e:  # noqa
        create_dag(nonexistent_dir)


def test_missing_dag_message(nonexistent_dir):
    try:
        create_dag(nonexistent_dir)
    except NonexistentDagDirError as e:
        assert str(e) == "DAG directory tests/dagz/nonexistent does not exist"
