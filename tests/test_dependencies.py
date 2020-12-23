import pytest
from gusty import get_spec_external_dependencies

@pytest.fixture
def sample_spec():
    spec = {'task_id': 'test_task',
            'external_dependencies': [
                {'dag1': 'task1'},
                {'dag1': 'task2'}
            ]}
    return spec

def test_get_spec_external_dependencies(sample_spec):
    external_dependencies = get_spec_external_dependencies(sample_spec)
    assert len(external_dependencies) == 2
