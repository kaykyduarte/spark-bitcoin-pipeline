from pathlib import Path

from pyspark.sql import SparkSession

from src.data_quality import (
    calcular_metricas,
    encontrar_gaps,
    encontrar_timestamps_duplicados,
    validar_ingestao,
)
from src.schema import schema

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = PROJECT_ROOT / "data/raw/btcusd_1-min_data.csv"
BRONZE_PATH = Path.home() / "spark-bitcoin-data" / "bronze" / "bitcoin_bronze"

BRONZE_MAPPING = {
    "timestamp": "Timestamp",
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "volume": "Volume",
}


def main():
    spark = (
        SparkSession.builder.master("local[2]").appName("validate-bronze").getOrCreate()
    )

    try:
        df_bronze = spark.read.parquet(str(BRONZE_PATH))

        df_raw = spark.read.csv(
            str(RAW_PATH), header=True, schema=schema, mode="PERMISSIVE"
        )

        metricas = calcular_metricas(df_bronze, BRONZE_MAPPING).first().asDict()

        duplicados = encontrar_timestamps_duplicados(df_bronze, BRONZE_MAPPING)
        metricas["total_duplicados"] = duplicados.count()

        print("\nMétricas da bronze:")
        for nome, valor in metricas.items():
            print(f"{nome}: {valor}")

        total_raw = df_raw.count()
        total_bronze = metricas["total_registros"]

        metricas["count_difference"] = abs(total_raw - total_bronze)

        validar_ingestao(metricas)
        print("\nValidacao de qualidade da bronze aprovada")

        print(f"Total de registros no raw: {total_raw}")
        print(f"Total de registros no bronze: {total_bronze}")
        print(f"Contagens iguais: {total_raw == total_bronze}")

        gaps_bronze = encontrar_gaps(df_bronze, BRONZE_MAPPING)

        print("\nGaps Bronze:")
        gaps_bronze.show()
        total_gaps = gaps_bronze.count()
        print(f"Quantidade de gaps: {total_gaps}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
