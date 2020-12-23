import pytest
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from gusty import get_yaml_specs, build_tasks

@pytest.fixture
def dag():
    default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

    dag = DAG(
        'tutorial',
        default_args=default_args,
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2),
        tags=['example'],
    )

    return dag

@pytest.fixture
def specs():
    specs = get_yaml_specs('examples/gusty_tutorial')
    return specs

def test_build_tasks(specs, dag):
    tasks = build_tasks(specs, dag)
    assert isinstance(tasks['print_date'], airflow.operators.bash.BashOperator)
