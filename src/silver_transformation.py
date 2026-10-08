from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.data_quality import (
    calcular_metricas,
    encontrar_timestamps_duplicados,
    validar_qualidade,
)

DATA_PATH = Path.home() / "spark-bitcoin-data"
BRONZE_PATH = DATA_PATH / "bronze" / "bitcoin_bronze"
SILVER_PATH = DATA_PATH / "silver" / "bitcoin_silver"

BRONZE_MAPPING = {
    "timestamp": "Timestamp",
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "volume": "Volume",
}


def transformar_bronze_em_silver(df_bronze):

    df_silver = df_bronze.toDF(*[coluna.lower() for coluna in df_bronze.columns])

    return df_silver.withColumn(
        "timestamp",
        F.timestamp_seconds(F.col("timestamp")),
    )


def main():

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark-bitcoin-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        df_bronze = spark.read.parquet(str(BRONZE_PATH))

        metricas = calcular_metricas(df_bronze, BRONZE_MAPPING).first().asDict()

        duplicados = encontrar_timestamps_duplicados(df_bronze, BRONZE_MAPPING)

        metricas["total_duplicados"] = duplicados.count()

        print("\nMétricas da bronze:")
        for nome, valor in metricas.items():
            print(f"{nome}: {valor}")

        validar_qualidade(metricas)
        print("\nQualidade da bronze aprovada.")

        df_silver = transformar_bronze_em_silver(df_bronze)

        SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)

        df_silver.write.mode("overwrite").parquet(str(SILVER_PATH))
        print(f"\nSilver salva em: {SILVER_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
