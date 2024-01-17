from contextlib import suppress

import pytest
from gusty import create_dags
from gusty.exceptions import GustyDagsImportError


##############
## FIXTURES ##
##############


@pytest.fixture(scope="session")
def multiple_dags_dir():
    return "tests/dags/multiple_dags"


@pytest.fixture(scope="session")
def multi_serial(multiple_dags_dir):
    dag_dict = {}
    with suppress(GustyDagsImportError):
        create_dags(
            multiple_dags_dir,
            dag_dict,
            schedule="0 11 * * *",
            default_args={"email": "default@gusty.com", "owner": "default"},
            wait_for_defaults={"timeout": 679},
        )
    return dag_dict


###########
## Tests ##
###########


def test_wait_for_defaults(multi_serial):
    serial_a = (
        multi_serial["dag_a"].task_dict["wait_for_dag_b_task_2"].__dict__["timeout"]
    )
    assert serial_a == 679


def test_kwargs(multi_serial):
    serial_c = multi_serial["dag_c"].__dict__["schedule_interval"]
    assert serial_c == "0 11 * * *"


def test_kwargs_override(multi_serial):
    # a
    serial_a = multi_serial["dag_a"].__dict__["schedule_interval"]
    # b
    serial_b = multi_serial["dag_b"].__dict__["schedule_interval"]
    assert serial_a == "0 0 * * *"
    # b
    assert serial_b == "0 0 * * *"


def test_default_args(multi_serial):
    serial_c_owner = multi_serial["dag_c"].task_dict["task_1"].__dict__["owner"]
    serial_c_email = multi_serial["dag_c"].task_dict["task_1"].__dict__["email"]
    assert serial_c_owner == "default"
    assert serial_c_email == "default@gusty.com"


def test_default_args_override(multi_serial):
    # a
    serial_a_owner = multi_serial["dag_a"].task_dict["task_1"].__dict__["owner"]
    serial_a_email = multi_serial["dag_a"].task_dict["task_1"].__dict__["email"]
    # b
    serial_b_owner = multi_serial["dag_b"].task_dict["task_1"].__dict__["owner"]
    serial_b_email = multi_serial["dag_b"].task_dict["task_1"].__dict__["email"]
    # a
    assert serial_a_owner == "a"
    assert serial_a_email == "a@gusty.com"
    # b
    assert serial_b_owner == "b"
    assert serial_b_email == "b@gusty.com"
