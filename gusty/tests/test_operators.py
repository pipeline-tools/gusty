import airflow
import os
import tempfile

from pathlib import Path

import pytest

@pytest.fixture
def tmp_dir():
    # pandas used to create a CSV, but could use python's built in csv module
    import pandas as pd

    # set up temporary airflow directory
    orig_home = os.environ.get("AIRFLOW_HOME")
    with tempfile.TemporaryDirectory() as tmp_dir:
        # set new env variables
        os.environ["AIRFLOW_HOME"] = orig_home or tmp_dir
        yield tmp_dir

def test_stub():
    assert True

@pytest.mark.xfail
def test_stub_xfail():
    assert False

@pytest.mark.skip
def test_stub_skip():
    print("should not run")
    assert False
