import pytest
from gusty import create_dag
from airflow.operators.empty import EmptyOperator

###############
## FIXTURES ##
###############


@pytest.fixture(scope="session")
def pydag_dir():
    return "tests/dags/pydag"


@pytest.fixture(scope="session")
def dag(pydag_dir):
    dag = create_dag(
        pydag_dir,
        default_args={"email": "default@gusty.com", "retries": 5},
        task_group_defaults={"prefix_group_id": True},
        wait_for_defaults={"poke_interval": 12},
    )
    return dag


@pytest.fixture(scope="session")
def py_task(dag):
    py_task = dag.task_dict["py_task"]
    return py_task


###########
## TESTS ##
###########


def test_py_task_exists(dag):
    assert "py_task" in dag.task_dict.keys()


def test_py_task_callable(py_task):
    callable = py_task.__dict__["python_callable"]
    res = callable()
    assert res == "hello python task"


def test_py_task_overrides(py_task):
    assert py_task.__dict__["email"] == "new_email@gusty.com"


def test_py_task_external_dependencies(py_task):
    assert "wait_for_DAG_a_whole_dag" in py_task.__dict__["upstream_task_ids"]


def test_py_task_dependencies(py_task):
    assert "direct_dep" in py_task.__dict__["upstream_task_ids"]
    assert "simple_leaf" in py_task.__dict__["downstream_task_ids"]


def test_py_dummy(dag):
    """
    This really tests to ensure that in theory the contents of the .py could
    be run by other operators as needed.
    """
    task = dag.task_dict["py_but_dummy"]
    assert isinstance(task, EmptyOperator)


def test_automatic_py(dag, py_task):
    task = dag.task_dict["simple_leaf"]
    assert type(py_task) == type(task)
