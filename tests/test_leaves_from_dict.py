import pytest
from gusty import create_dag

##############
## FIXTURES ##
##############


@pytest.fixture(scope="session")
def leaves_from_dag_dir():
    return "tests/dags/leaves_from"


@pytest.fixture(scope="session")
def dag(leaves_from_dag_dir):
    dag = create_dag(
        leaves_from_dag_dir,
        leaf_tasks_from_dict={
            "dict_leaf": {"operator": "airflow.operators.empty.EmptyOperator"},
            "overwritten_leaf": {"operator": "airflow.operators.empty.EmptyOperator"},
        },
    )
    return dag


###########
## Tests ##
###########


def test_only_leaves_are_leaves(dag):
    task_dict = dag.__dict__["task_dict"]
    # These should not be leaves
    assert task_dict["python_a"] not in dag.leaves
    assert task_dict["python_b"] not in dag.leaves
    # These should be leaves
    assert task_dict["yaml_leaf"] in dag.leaves
    assert task_dict["dict_leaf"] in dag.leaves
    assert task_dict["overwritten_leaf"] in dag.leaves


def test_yaml_spec_overrides_leaf_dict(dag):
    assert (
        dag.__dict__["task_dict"]["overwritten_leaf"].__dict__["bash_command"]
        == "echo overwritten"
    )
