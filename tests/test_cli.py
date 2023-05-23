import pytest
import os
from datetime import timedelta
from click.testing import CliRunner

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

from gusty.cli import cli, sample_tasks
from gusty import create_dag


@pytest.fixture()
def dag_name():
    return "cli_dag"


@pytest.fixture(scope="session")
def output_path(tmp_path_factory):
    return tmp_path_factory.mktemp("cli_output")


@pytest.fixture()
def cli_runner():
    return CliRunner()


@pytest.fixture()
def cli_result(dag_name, cli_runner, output_path):
    with cli_runner.isolated_filesystem(temp_dir=output_path):
        result = cli_runner.invoke(
            cli,
            ["use", "create-dag", f"--name={dag_name}", f"--dags-dir={output_path}"],
        )
    return result


@pytest.fixture()
def dag(output_path, dag_name):
    dag_path = os.path.join(output_path, dag_name)
    return create_dag(dag_path)


def test_cli_result(cli_result):
    assert cli_result.exit_code == 0


def test_create_dag_file(dag_name, output_path):
    # Get expected contents of the create_dag file
    (
        create_dag_filename,
        create_dag_file_expected_contents_list,
    ) = sample_tasks.create_dag_file(dag_name)
    create_dag_file_expected_contents = "".join(create_dag_file_expected_contents_list)

    # Check that the CLI-made create_dag file has expected contents
    create_dag_file_path = os.path.join(output_path, create_dag_filename)
    assert os.path.exists(create_dag_file_path)

    with open(create_dag_file_path, "r") as f:
        create_dag_file_contents = f.read()
        assert create_dag_file_contents == create_dag_file_expected_contents


def test_task_files(dag_name, output_path):
    dag_path = os.path.join(output_path, dag_name)
    assert os.path.exists(dag_path)

    for (
        task_file_name,
        task_file_expected_contents_list,
    ) in sample_tasks.dag_contents_map.items():
        # Get expected task file contents
        task_file_expected_contents = "".join(task_file_expected_contents_list)
        task_file_path = os.path.join(dag_path, task_file_name)
        assert os.path.exists(task_file_path)

        # Check that task files have expected contents
        with open(task_file_path, "r") as f:
            task_file_contents = f.read()
            assert task_file_contents == task_file_expected_contents


def test_dag_object(dag):
    assert isinstance(dag, DAG)


def test_python_task(dag):
    assert isinstance(dag.task_dict["hi"], PythonOperator)
    assert dag.task_dict["hi"].python_callable() == "hi"


def test_sql_task(dag):
    assert isinstance(dag.task_dict["hey"], SqliteOperator)
    assert dag.task_dict["hey"].sql == "SELECT 'hey'"


def test_yml_task(dag):
    assert isinstance(dag.task_dict["hello"], BashOperator)
    assert dag.task_dict["hello"].bash_command == "echo hello"


def test_latest_only_root(dag):
    assert len(dag.roots) == 1
    assert dag.roots[0].task_id == "latest_only"


def test_metadata(dag):
    assert dag.__dict__["default_args"]["retry_delay"] == timedelta(minutes=5)
