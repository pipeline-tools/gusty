import click
import os
from gusty.cli.utils import get_dags_directory
from gusty.cli.sample_tasks import create_dag_file, dag_contents_map, create_dags_file

@click.group()
def cli():
    pass

@click.command()
@click.argument('func', type=click.Choice(['create-dag', 'create-dags']))
@click.option('--name', '-n', type=str)
@click.option('--dags-dir', '-d', type=click.Path())
def use(func, name, dags_dir):

    if not name:
        name = 'hello_dag' if func == 'create-dag' else 'gusty_dags'

    # dags directory defaults to current directory if not specified
    # and is not the current, parent, or child directory.
    if not dags_dir:
        dags_dir = dags_dir if dags_dir else get_dags_directory(dags_dir)

    dag_path = os.path.join(dags_dir, name)
    if func == 'create-dags':
        dag_path = os.path.join(dags_dir, name, 'hello_dag')
        os.makedirs(dag_path)
    else:
        os.mkdir(dag_path)

    for filename, contents in dag_contents_map.items():
        fpath = os.path.join(dag_path, filename)
        with open(fpath, 'x') as f:
            f.writelines(contents)

    full_dag_path = os.path.abspath(dag_path)
    create_dag_filename, create_dag_contents = (
        create_dag_file(name, full_dag_path) if func == 'create-dag' else create_dags_file(name, full_dag_path)
    )
    create_file_fpath = os.path.join(dags_dir, create_dag_filename)
    with open(create_file_fpath, 'x') as f:
        f.writelines(create_dag_contents)

cli.add_command(use)