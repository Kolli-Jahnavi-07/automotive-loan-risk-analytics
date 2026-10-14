import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    from loanrisk.spark_silver import spark_session

    os.environ.setdefault("SPARK_MASTER", "local[2]")
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    s = spark_session("loan-risk-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
