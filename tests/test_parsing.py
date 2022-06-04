import pytest
from datetime import timedelta
from gusty.utils import days_ago
from gusty import create_dag
from gusty.parsing import parse

##############
## FIXTURES ##
##############


@pytest.fixture(scope="session")
def parsing_dag_dir():
    return "tests/dags/parsing"


@pytest.fixture(scope="session")
def dag(parsing_dag_dir):
    dag = create_dag(
        parsing_dag_dir,
        description="A dag with some custom parsing functions.",
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
        parse_hooks={
            ".py": lambda file_path: {
                "operator": "airflow.operators.python.PythonOperator",
                "python_callable": lambda: "this was custom",
            },
        },
    )
    return dag


@pytest.fixture(scope="session")
def custom_task(dag):
    custom_task = dag.task_dict["a_parse_hook_task"]
    return custom_task


@pytest.fixture(scope="session")
def sql_task(dag):
    custom_task = dag.task_dict["sql_task"]
    return custom_task


###########
## Tests ##
###########


def test_read_yaml_spec():
    yaml_spec = parse("tests/dags/no_metadata/top_level_task.yml")
    assert yaml_spec["task_id"] == "top_level_task"
    assert yaml_spec["file_path"] == "tests/dags/no_metadata/top_level_task.yml"
    assert "operator" in yaml_spec.keys()
    assert "bash_command" in yaml_spec.keys()


def test_parse_hooks(custom_task):
    callable = custom_task.__dict__["python_callable"]
    res = callable()
    assert res == "this was custom"


def test_sql_parse(sql_task):
    assert sql_task.sql == "SELECT *\nFROM gusty_table"
