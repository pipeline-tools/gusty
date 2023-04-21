import click
import os
from gusty.cli.sample_tasks import create_dag_file, dag_contents_map

@click.group()
def cli():
    pass

@click.command()
@click.argument('dag_name', type=click.Path())
@click.option('--dags-dir', default='dags')
def create_dag(dag_name, dags_dir):
    # Look at current, parent, and child directory for dags directory

    dag_path = os.path.join(dags_dir, dag_name)
    os.mkdir(dag_path)

    for filename, contents in dag_contents_map.items():
        with open(os.path.join(dag_path, filename), 'x') as f:
            f.writelines(contents)

    create_dag_filename, create_dag_contents = create_dag_file(dag_name)
    with open(os.path.join(dags_dir, create_dag_filename), 'x') as f:
        f.writelines(create_dag_contents)

cli.add_command(create_dag)