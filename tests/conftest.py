from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"

pytest_plugins = ["pytester"]


def fixture_text(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def aqe_final_text() -> str:
    return fixture_text("aqe_final_broadcast.txt")


@pytest.fixture
def smj_udf_window_text() -> str:
    return fixture_text("smj_udf_window.txt")


def _spark_available() -> bool:
    try:
        import pyspark  # noqa: F401
    except Exception:
        return False
    return shutil.which("java") is not None or bool(os.environ.get("JAVA_HOME"))


@pytest.fixture(scope="session")
def spark():
    if not _spark_available():
        pytest.skip("pyspark and Java are required for Spark integration tests")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("planlock-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
