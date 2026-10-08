from pathlib import Path

from pyspark.sql import SparkSession

from src.schema import schema

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_PATH = PROJECT_ROOT / "data" / "raw" / "btcusd_1-min_data.csv"

BRONZE_PATH = Path.home() / "spark-bitcoin-data" / "bronze" / "bitcoin_bronze"


def main():
    try:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("spark-bitcoin-bronze")
            .getOrCreate()
        )

        df = spark.read.csv(
            str(RAW_PATH),
            header=True,
            schema=schema,
            mode="PERMISSIVE",
        )

        BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)

        df.write.mode("overwrite").parquet(str(BRONZE_PATH))

        print(f"Parquet salvo em: {BRONZE_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
