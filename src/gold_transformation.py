from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.data_quality import (
    calcular_metricas,
    encontrar_timestamps_duplicados,
    validar_qualidade,
    validar_record_count_gold,
)

SILVER_PATH = Path.home() / "spark-bitcoin-data" / "silver" / "bitcoin_silver"

GOLD_PATH = Path.home() / "spark-bitcoin-data" / "gold" / "bitcoin_hourly"

GOLD_MAPPING = {
    "timestamp": "hour",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
}


def transformar_silver_em_gold(df_silver):
    return (
        df_silver.withColumn(
            "hour",
            F.date_trunc("hour", F.col("timestamp")),
        )
        .groupBy("hour")
        .agg(
            F.min_by("open", "timestamp").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by("close", "timestamp").alias("close"),
            F.sum("volume").alias("volume"),
            F.count("*").alias("record_count"),
        )
    )


def main():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark-bitcoin-gold")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        df_silver = spark.read.parquet(str(SILVER_PATH))

        df_gold = transformar_silver_em_gold(df_silver)

        metricas_gold = (
            calcular_metricas(
                df_gold,
                GOLD_MAPPING,
            )
            .first()
            .asDict()
        )

        duplicados = encontrar_timestamps_duplicados(
            df_gold,
            GOLD_MAPPING,
        )

        metricas_gold["total_duplicados"] = duplicados.count()

        print("\nMetricas da gold:")
        for nome, valor in metricas_gold.items():
            print(f"{nome}: {valor}")

        validar_qualidade(metricas_gold)
        print("\nValidacao de qualidade gold aprovada.")

        limites = df_silver.agg(
            F.date_trunc("hour", F.min("timestamp")).alias("primeira_hora"),
            F.date_trunc("hour", F.max("timestamp")).alias("ultima_hora"),
        ).first()

        validar_record_count_gold(
            df_gold,
            limites["primeira_hora"],
            limites["ultima_hora"],
        )
        print("Validacao de record_count da gold aprovada.")

        GOLD_PATH.parent.mkdir(parents=True, exist_ok=True)

        df_gold.write.mode("overwrite").parquet(str(GOLD_PATH))
        print(f"\nGold salva em: {GOLD_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
