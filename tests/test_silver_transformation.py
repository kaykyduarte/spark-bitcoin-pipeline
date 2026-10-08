import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

from src.silver_transformation import transformar_bronze_em_silver


def test_transformar_bronze_em_silver(spark):

    dados = [
        (3600, 100.0, 120.0, 90.0, 112.0, 30.0),
    ]

    df_bronze = spark.createDataFrame(
        dados,
        schema="""
            Timestamp LONG,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            Close DOUBLE,
            Volume DOUBLE
        """,
    )

    df_silver = transformar_bronze_em_silver(df_bronze)

    assert df_silver.columns == [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    assert isinstance(
        df_silver.schema["timestamp"].dataType,
        TimestampType,
    )

    for coluna in ("open", "high", "low", "close", "volume"):
        assert isinstance(
            df_silver.schema[coluna].dataType,
            DoubleType,
        )

    linhas = df_silver.select(
        F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss").alias("timestamp_utc"),
        "open",
        "high",
        "low",
        "close",
        "volume",
    ).collect()

    assert len(linhas) == 1

    linha = linhas[0]

    assert linha["timestamp_utc"] == "1970-01-01 01:00:00"
    assert linha["open"] == pytest.approx(100.0)
    assert linha["high"] == pytest.approx(120.0)
    assert linha["low"] == pytest.approx(90.0)
    assert linha["close"] == pytest.approx(112.0)
    assert linha["volume"] == pytest.approx(30.0)
