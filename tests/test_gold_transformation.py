from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F

from src.gold_transformation import transformar_silver_em_gold

SILVER_SCHEMA = """
    timestamp TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE
"""


def test_transformar_silver_em_gold(spark):
    dados = [
        (
            datetime(2026, 10, 7, 10, 2, tzinfo=timezone.utc),
            98.0,
            115.0,
            92.0,
            112.0,
            30.0,
        ),
        (
            datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc),
            100.0,
            110.0,
            95.0,
            105.0,
            10.0,
        ),
        (
            datetime(2026, 10, 7, 10, 1, tzinfo=timezone.utc),
            105.0,
            120.0,
            90.0,
            98.0,
            20.0,
        ),
    ]

    df_silver = spark.createDataFrame(
        dados,
        schema=SILVER_SCHEMA,
    )

    df_gold = transformar_silver_em_gold(df_silver)
    linha = df_gold.first()

    assert df_gold.count() == 1
    assert linha is not None

    hora_esperada = datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc)

    assert linha["hour"].timestamp() == hora_esperada.timestamp()

    assert linha["open"] == pytest.approx(100.0)
    assert linha["high"] == pytest.approx(120.0)
    assert linha["low"] == pytest.approx(90.0)
    assert linha["close"] == pytest.approx(112.0)
    assert linha["volume"] == pytest.approx(60.0)
    assert linha["record_count"] == 3


def test_transformar_silver_em_gold_separa_horas(spark):

    dados = [
        (
            datetime(2026, 10, 7, 11, 1, tzinfo=timezone.utc),
            205.0,
            220.0,
            198.0,
            215.0,
            40.0,
        ),
        (
            datetime(2026, 10, 7, 10, 59, tzinfo=timezone.utc),
            105.0,
            120.0,
            90.0,
            112.0,
            20.0,
        ),
        (
            datetime(2026, 10, 7, 11, 0, tzinfo=timezone.utc),
            200.0,
            210.0,
            190.0,
            205.0,
            30.0,
        ),
        (
            datetime(2026, 10, 7, 10, 58, tzinfo=timezone.utc),
            100.0,
            110.0,
            95.0,
            105.0,
            10.0,
        ),
    ]

    df_silver = spark.createDataFrame(dados, schema=SILVER_SCHEMA)

    df_gold = transformar_silver_em_gold(df_silver)

    linhas = (
        df_gold.orderBy("hour")
        .select(
            F.date_format("hour", "yyyy-MM-dd HH:mm:ss").alias("hour_utc"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "record_count",
        )
        .collect()
    )

    assert len(linhas) == 2

    esperados = [
        {
            "hour_utc": "2026-10-07 10:00:00",
            "open": 100.0,
            "high": 120.0,
            "low": 90.0,
            "close": 112.0,
            "volume": 30.0,
            "record_count": 2,
        },
        {
            "hour_utc": "2026-10-07 11:00:00",
            "open": 200.0,
            "high": 220.0,
            "low": 190.0,
            "close": 215.0,
            "volume": 70.0,
            "record_count": 2,
        },
    ]

    for linha, esperado in zip(linhas, esperados):
        assert linha["hour_utc"] == esperado["hour_utc"]
        assert linha["record_count"] == esperado["record_count"]

        for coluna in ("open", "high", "low", "close", "volume"):
            assert linha[coluna] == pytest.approx(esperado[coluna]), (
                f"Valor incorreto em {coluna} na hora {esperado['hour_utc']}"
            )
