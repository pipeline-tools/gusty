import pytest
import os
from click.testing import CliRunner
from gusty.cli import cli, sample_tasks

@pytest.fixture()
def cli_runner():
    return CliRunner()

def test_cli_create_dag(cli_runner, tmp_path):
    # Create an isolated directory in which to create DAG
    with cli_runner.isolated_filesystem(temp_dir=tmp_path):
        dag_name = 'cli_dag'
        result = runner.invoke(cli, ['use', 'create-dag', 'cli_dag', f'--dags-dir={tmp_path}'])

        assert result.exit_code == 0

        # Get expected contents of the create_dag file
        create_dag_filename, create_dag_file_expected_contents_list = sample_tasks.create_dag_file(dag_name)
        create_dag_file_expected_contents = ''.join(create_dag_file_expected_contents_list)

        # Check that the CLI-made create_dag file has expected contents
        create_dag_file_path = os.path.join(tmp_path, create_dag_filename)
        assert os.path.exists(create_dag_file_path)

        with open(create_dag_file_path, 'r') as f:
            create_dag_file_contents = f.read()
            assert create_dag_file_contents == create_dag_file_expected_contents

        # Check that dag and tasks exist
        dag_path = os.path.join(tmp_path, dag_name)
        assert os.path.exists(dag_path)

        for task_file_name, task_file_expected_contents_list in sample_tasks.dag_contents_map.items():
            # Get expected task file contents
            task_file_expected_contents = ''.join(task_file_expected_contents_list)
            task_file_path = os.path.join(dag_path, task_file_name)
            assert os.path.exists(task_file_path)

            # Check that task files have expected contents
            with open(task_file_path, 'r') as f:
                task_file_contents = f.read()
                assert task_file_contents == task_file_expected_contents

