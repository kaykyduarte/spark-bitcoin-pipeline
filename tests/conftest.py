import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pipeline-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session

    session.stop()
