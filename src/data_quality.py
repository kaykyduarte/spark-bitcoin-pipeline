from pyspark.sql import functions as F
from pyspark.sql.types import LongType, TimestampType
from pyspark.sql.window import Window


def calcular_metricas(df, column_mapping):
    colunas = {
        nome_logico: F.col(nome_fisico)
        for nome_logico, nome_fisico in column_mapping.items()
    }

    regras = {
        "high_below_open": colunas["high"] < colunas["open"],
        "high_below_close": colunas["high"] < colunas["close"],
        "high_below_low": colunas["high"] < colunas["low"],
        "low_above_open": colunas["low"] > colunas["open"],
        "low_above_close": colunas["low"] > colunas["close"],
        "negative_volume": colunas["volume"] < 0,
    }

    nomes_metricas_nulos = {
        "timestamp": "Timestamp_nulls",
        "open": "Open_nulls",
        "high": "High_nulls",
        "low": "Low_nulls",
        "close": "Close_nulls",
        "volume": "Volume_nulls",
    }

    contadores_nulos = [
        F.count(F.when(colunas[nome_logico].isNull(), 1)).alias(nome_metrica)
        for nome_logico, nome_metrica in nomes_metricas_nulos.items()
    ]

    contadores_regras = [
        F.coalesce(
            F.sum(F.when(condicao, 1).otherwise(0)),
            F.lit(0),
        ).alias(nome)
        for nome, condicao in regras.items()
    ]

    metricas = df.agg(
        F.count("*").alias("total_registros"),
        *contadores_nulos,
        *contadores_regras,
    )

    return metricas.withColumn(
        "total_nulls",
        sum(
            (F.col(nome_metrica) for nome_metrica in nomes_metricas_nulos.values()),
            F.lit(0),
        ),
    )


def encontrar_timestamps_duplicados(df, column_mapping):
    timestamp_col = F.col(column_mapping["timestamp"])

    return (
        df.filter(timestamp_col.isNotNull())
        .groupBy(timestamp_col)
        .count()
        .filter(F.col("count") > 1)
    )


def encontrar_gaps(df, column_mapping):
    nome_timestamp = column_mapping["timestamp"]
    tipo_timestamp = df.schema[nome_timestamp].dataType

    if isinstance(tipo_timestamp, LongType):
        # Long type representa segundos unix.
        timestamp_seconds = F.col(nome_timestamp)

    elif isinstance(tipo_timestamp, TimestampType):
        timestamp_seconds = F.unix_seconds(F.col(nome_timestamp))

    else:
        raise TypeError(
            f"coluna '{nome_timestamp}' deve ser LongType ou TimestampType. "
            f"Tipo encontrado: {tipo_timestamp.simpleString()}"
        )

    base = df.select(
        F.col(nome_timestamp),
        timestamp_seconds.alias("_timestamp_seconds"),
    ).filter(F.col(nome_timestamp).isNotNull())

    janela = Window.orderBy("_timestamp_seconds")

    return (
        base.withColumn(
            "previous_timestamp_seconds",
            F.lag("_timestamp_seconds").over(janela),
        )
        .withColumn(
            "delta_seconds",
            F.col("_timestamp_seconds") - F.col("previous_timestamp_seconds"),
        )
        .filter(F.col("delta_seconds") > 60)
        .select(
            nome_timestamp,
            "previous_timestamp_seconds",
            "delta_seconds",
        )
    )


def validar_qualidade(metricas):
    nomes = [
        "Timestamp_nulls",
        "Open_nulls",
        "High_nulls",
        "Low_nulls",
        "Close_nulls",
        "Volume_nulls",
        "high_below_open",
        "high_below_close",
        "high_below_low",
        "low_above_open",
        "low_above_close",
        "negative_volume",
        "total_duplicados",
    ]

    falhas = {nome: metricas[nome] for nome in nomes if metricas[nome] > 0}

    if falhas:
        detalhes = ", ".join(f"{nome}={valor}" for nome, valor in falhas.items())

        raise ValueError(f"Validacao de qualidade falhou: {detalhes}")


def validar_ingestao(metricas):
    validar_qualidade(metricas)

    diferenca = metricas["count_difference"]

    if diferenca > 0:
        raise ValueError(f"Falha da ingestao: count_difference={diferenca}")


def validar_record_count_gold(df_gold, primeira_hora, ultima_hora):
    hora = F.col("hour")
    quantidade = F.col("record_count")

    hora_interna = (hora != F.lit(primeira_hora)) & (hora != F.lit(ultima_hora))

    horas_invalidas = df_gold.filter(
        (quantidade < 1) | (quantidade > 60) | (hora_interna & (quantidade != 60))
    )

    if horas_invalidas.limit(1).count() > 0:
        raise ValueError(
            "Gold invalido: record_count deve estar entre 1 e 60, "
            "e horas internas devem ter exatamente 60 registros."
        )
