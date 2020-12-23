import pytest
from gusty import GustyDAG


@pytest.fixture
def gustydag():
    dag = GustyDAG('examples/gusty_tutorial')
    return dag

def test_dag_tasks(gustydag):
    assert len(gustydag._task_group.children) == 5 # latest only by default + one external dependency in sleep job

def test_dag_task_dependencies(gustydag):
    assert gustydag._task_group.children['sleep']._upstream_task_ids == {'print_date'}

def test_dag_task_dependencies(gustydag):
    assert gustydag._task_group.children['sleep'].retries == 3
