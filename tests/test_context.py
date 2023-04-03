import pytest
from gusty.utils.context import (
    generate_runner_context,
    generate_loader_constructors,
    generate_user_defined_macros,
)

# (Everything in order of priority)
# The loader (!) needs:
#   - Airflow user-defined macros
#   - any additional dag_constructors
#   - default constructors
# The runner ({{}}) needs:
#   - Airflow user-defined macros
#   - any additional dag_constructors
#   - Airflow var.value, var.json, and macros
#   - default constructors
# Airflow user-defined macros needs:
#   - Airflow user-defined macros
#   - any additional dag_constructors


@pytest.fixture()
def user_provided_macros():
    return {"udf": lambda: "udf", "var": lambda: "var"}


@pytest.fixture()
def dag_constructors():
    return {
        "!dc": lambda: "dc",
        "!udf": lambda: "nonono",
        "!datetime": lambda: "yesyesyes",
    }


@pytest.fixture()
def loader_constructors(user_provided_macros, dag_constructors):
    return generate_loader_constructors(user_provided_macros, dag_constructors)


@pytest.fixture()
def runner_context(loader_constructors):
    return generate_runner_context(loader_constructors)


@pytest.fixture()
def user_defined_macros(user_provided_macros, dag_constructors):
    return generate_user_defined_macros(user_provided_macros, dag_constructors)


def test_runner_context_macros_over_constructors(runner_context):
    # user_provided_macros are preferred over dag_constructors
    assert runner_context["udf"]() == "udf"


def test_runner_context_constructors_in_context(runner_context):
    # dag_constructors make it into runner context
    assert runner_context["dc"]() == "dc"


def test_runner_context_dag_con_over_default_con(runner_context):
    # dag_constructors are prefered over default_constructors
    assert runner_context["datetime"]() == "yesyesyes"


# Cannot include macros because Airflow uses deepcopy
# def test_runner_context_macros_in_context(runner_context):
#     # airflow_default_macros make it into runner context
#     assert runner_context["macros"].ds_add("2022-01-01", 1) == "2022-01-02"


def test_runner_context_udf_over_default_macros(runner_context):
    # user_defined_macros are peffered over airflow_default_macros
    assert runner_context["var"]() == "var"


def test_user_defined_macros_macros_over_constructors(user_defined_macros):
    # user_provided_macros are preferred over dag_constructors
    assert user_defined_macros["udf"]() == "udf"


def test_user_defined_macros_constructors_in_macros(user_defined_macros):
    # dag_constructors make it into user_defined_macros
    assert user_defined_macros["dc"]() == "dc"
