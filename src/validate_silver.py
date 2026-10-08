from pathlib import Path

from pyspark.sql import SparkSession

from src.data_quality import (
    calcular_metricas,
    encontrar_gaps,
    encontrar_timestamps_duplicados,
    validar_qualidade,
)

DATA_PATH = Path.home() / "spark-bitcoin-data"
BRONZE_PATH = DATA_PATH / "bronze" / "bitcoin_bronze"
SILVER_PATH = DATA_PATH / "silver" / "bitcoin_silver"

SILVER_MAPPING = {
    "timestamp": "timestamp",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
}


def main():

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark-bitcoin-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        df_bronze = spark.read.parquet(str(BRONZE_PATH))
        df_silver = spark.read.parquet(str(SILVER_PATH))

        metricas = (
            calcular_metricas(
                df_silver,
                SILVER_MAPPING,
            )
            .first()
            .asDict()
        )

        duplicados = encontrar_timestamps_duplicados(
            df_silver,
            SILVER_MAPPING,
        )

        metricas["total_duplicados"] = duplicados.count()

        validar_qualidade(metricas)

        for nome, valor in metricas.items():
            print(f"{nome}: {valor}")

        df_silver.printSchema()

        total_bronze = df_bronze.count()
        total_silver = df_silver.count()

        gaps_silver = encontrar_gaps(df_silver, SILVER_MAPPING)
        gaps_silver.show(truncate=False)

        print(f"Bronze: {total_bronze}")
        print(f"Silver: {total_silver}")
        print(f"Contagens iguais: {total_bronze == total_silver}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
