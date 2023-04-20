import click
import os

@click.group()
def cli():
    pass

@click.command()
@click.argument('object', type=click.Choice(['task', 'dag'], case_sensitive=False))
@click.argument('filepath', type=click.Path())
def create(object, filepath):
    click.echo(f'Creating {object} at {filepath}.')

    if object == 'dag':
        os.mkdir(filepath)
        open(os.path.join(filepath, 'METADATA.yml'), 'a').close()

    if object == 'task':
        os.touch(filepath)

    click.echo('Done.')

cli.add_command(create)