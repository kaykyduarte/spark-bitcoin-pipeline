from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F

from src.data_quality import (
    calcular_metricas,
    encontrar_gaps,
    encontrar_timestamps_duplicados,
    validar_record_count_gold,
)


def test_validar_record_count_gold_rejeita_hora_interna_incompleta(spark):

    dados = [
        (datetime(2026, 10, 7, 11, 0, tzinfo=timezone.utc), 59),
    ]

    df_gold = spark.createDataFrame(
        dados,
        schema="hour TIMESTAMP, record_count LONG",
    )

    primeira_hora = datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc)
    ultima_hora = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="record_count"):
        validar_record_count_gold(
            df_gold,
            primeira_hora,
            ultima_hora,
        )


def test_validar_record_count_gold_permite_primeira_hora_parcial(spark):

    primeira_hora = datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc)
    ultima_hora = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)

    df_gold = spark.createDataFrame(
        [(primeira_hora, 59)],
        schema="hour TIMESTAMP, record_count LONG",
    )

    validar_record_count_gold(
        df_gold,
        primeira_hora,
        ultima_hora,
    )


def test_validar_record_count_gold_rejeita_fronteira_acima_de_60(spark):

    primeira_hora = datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc)
    ultima_hora = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)

    df_gold = spark.createDataFrame(
        [(primeira_hora, 61)],
        schema="hour TIMESTAMP, record_count LONG",
    )

    with pytest.raises(ValueError, match="record_count"):
        validar_record_count_gold(
            df_gold,
            primeira_hora,
            ultima_hora,
        )


def test_validar_record_count_gold_rejeita_fronteira_com_zero_registros(spark):

    primeira_hora = datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc)
    ultima_hora = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)

    df_gold = spark.createDataFrame(
        [(primeira_hora, 0)],
        schema="hour TIMESTAMP, record_count LONG",
    )

    with pytest.raises(ValueError, match="record_count"):
        validar_record_count_gold(
            df_gold,
            primeira_hora,
            ultima_hora,
        )


def test_calcular_metricas_conta_high_invalido_e_volume_negativo(spark):

    dados = [
        (3600, 100.0, 90.0, 80.0, 85.0, -5.0),
    ]

    df = spark.createDataFrame(
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

    column_mapping = {
        "timestamp": "timestamp",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    }

    metricas = calcular_metricas(df, column_mapping).first().asDict()

    assert metricas["total_registros"] == 1

    assert metricas["high_below_open"] == 1
    assert metricas["negative_volume"] == 1

    assert metricas["high_below_close"] == 0
    assert metricas["high_below_low"] == 0
    assert metricas["low_above_open"] == 0
    assert metricas["low_above_close"] == 0

    assert metricas["total_nulls"] == 0


def test_calcular_metricas_conta_close_nulo(spark):

    dados = [
        (3600, 100.0, 120.0, 90.0, None, 10.0),
    ]

    df = spark.createDataFrame(
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

    column_mapping = {
        "timestamp": "timestamp",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    }

    metricas = calcular_metricas(df, column_mapping).first().asDict()

    assert metricas["Close_nulls"] == 1
    assert metricas["total_nulls"] == 1
    assert metricas["high_below_close"] == 0
    assert metricas["low_above_close"] == 0


def test_encontrar_timestamps_duplicados_retorna_apenas_repetidos(spark):

    dados = [
        (datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc), 100.0),
        (datetime(2026, 10, 7, 10, 1, tzinfo=timezone.utc), 101.0),
        (datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc), 102.0),
    ]

    df = spark.createDataFrame(
        dados,
        schema="timestamp TIMESTAMP, open DOUBLE",
    )

    column_mapping = {"timestamp": "timestamp"}

    duplicados = encontrar_timestamps_duplicados(df, column_mapping)

    linhas = duplicados.select(
        F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss").alias("timestamp_utc"),
        "count",
    ).collect()

    assert len(linhas) == 1
    assert linhas[0]["timestamp_utc"] == "2026-10-07 10:00:00"
    assert linhas[0]["count"] == 2


def test_encontrar_gaps_identifica_intervalo_em_unix_seconds(spark):

    dados = [(0,), (60,), (180,)]

    df = spark.createDataFrame(
        dados,
        schema="Timestamp LONG",
    )

    column_mapping = {"timestamp": "Timestamp"}

    linhas = encontrar_gaps(df, column_mapping).collect()

    assert len(linhas) == 1

    linha = linhas[0]

    assert linha["Timestamp"] == 180
    assert linha["previous_timestamp_seconds"] == 60
    assert linha["delta_seconds"] == 120


def test_encontrar_gaps_identifica_intervalo_em_timestamp(spark):

    primeiro = datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc)
    anterior = datetime(2026, 10, 7, 10, 1, tzinfo=timezone.utc)
    atual = datetime(2026, 10, 7, 10, 3, tzinfo=timezone.utc)

    df_silver = spark.createDataFrame(
        [(primeiro,), (anterior,), (atual,)], schema="timestamp TIMESTAMP"
    )

    SILVER_MAPPING = {"timestamp": "timestamp"}

    gaps = encontrar_gaps(df_silver, SILVER_MAPPING)

    linhas = gaps.select(
        F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss").alias("timestamp_utc"),
        "previous_timestamp_seconds",
        "delta_seconds",
    ).collect()

    assert len(linhas) == 1

    linha = linhas[0]

    assert linha["timestamp_utc"] == "2026-10-07 10:03:00"
    assert linha["previous_timestamp_seconds"] == int(anterior.timestamp())
    assert linha["delta_seconds"] == 120


def test_encontrar_gaps_rejeita_timestamp_string(spark):

    df = spark.createDataFrame([("2026-10-07 10:00:00",)], schema="timestamp STRING")

    column_mapping = {"timestamp": "timestamp"}

    with pytest.raises(TypeError, match="LongType ou TimestampType"):
        encontrar_gaps(df, column_mapping)
