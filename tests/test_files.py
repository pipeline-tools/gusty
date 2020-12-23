import pytest
from gusty import get_files, read_yaml_spec, get_yaml_specs

def test_get_files():
    files = get_files('examples/gusty_tutorial')
    assert 'examples/gusty_tutorial/print_date.yml' in files
    assert 'examples/gusty_tutorial/sleep.yml' in files
    assert 'examples/gusty_tutorial/templated.yml' in files

def test_read_yaml_spec():
    yaml_spec = read_yaml_spec('examples/gusty_tutorial/print_date.yml')
    assert yaml_spec['task_id'] == 'print_date'
    assert yaml_spec['file_path'] == 'examples/gusty_tutorial/print_date.yml'
    assert 'operator' in yaml_spec.keys()
    assert 'bash_command' in yaml_spec.keys()

def test_get_yaml_specs():
    yaml_specs = get_yaml_specs('examples/gusty_tutorial')
    assert len(yaml_specs) == 3
    assert [*map(lambda x: isinstance(x, dict), yaml_specs)]
