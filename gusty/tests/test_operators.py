import airflow
import os
import tempfile

from pathlib import Path

import pytest

# will set in travis, but adding a default just in case
os.environ.setdefault(
    "AIRFLOW_CONN_POSTGRES_DEFAULT",
    "postgresql://postgres:@localhost:5433/postgres"
    )

@pytest.fixture
def tmp_dir():
    # pandas used to create a CSV, but could use python's built in csv module
    import pandas as pd

    # set up temporary airflow directory
    orig_home = os.environ.get("AIRFLOW_HOME")
    with tempfile.TemporaryDirectory() as tmp_dir:
        # set new env variables
        os.environ["AIRFLOW_HOME"] = tmp_dir

        # create csv directory
        csv_dir = Path(tmp_dir) / 'dags' / 'csv'
        csv_dir.mkdir(parents=True, exist_ok=False)

        yield tmp_dir

    # there is a better way to ensure we reset to original values
    if orig_home is not None:
        os.environ["AIRFLOW_HOME"] = orig_home



def test_stub():
    assert True

@pytest.mark.xfail
def test_stub_xfail():
    assert False

@pytest.mark.skip
def test_stub_skip():
    print("should not run")
    assert False
