import pytest
from gusty import get_operator_location, get_operator_name, get_operator_module


@pytest.fixture
def operator_string():
    return "airflow.operators.bash.BashOperator"


def test_location(operator_string):
    location = get_operator_location(operator_string)
    assert location == "airflow"


def test_operator_name(operator_string):
    operator_name = get_operator_name(operator_string)
    assert operator_name == "BashOperator"


def test_operator_module(operator_string):
    operator_module = get_operator_module(operator_string)
    assert operator_module == "airflow.operators.bash"


def test_module_fail():
    with pytest.raises(AssertionError):
        get_operator_module("BashOperator")
