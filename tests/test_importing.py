import pytest
from gusty import __get_operator

def test_get_operator():
    operator = __get_operator('airflow.operators.bash.BashOperator')
    assert operator.__name__ == 'BashOperator'
