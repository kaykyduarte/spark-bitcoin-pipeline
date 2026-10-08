from pyspark.sql.types import DoubleType, LongType, StructField, StructType

schema = StructType(
    [
        StructField("Timestamp", LongType(), nullable=True),
        StructField("Open", DoubleType(), nullable=True),
        StructField("High", DoubleType(), nullable=True),
        StructField("Low", DoubleType(), nullable=True),
        StructField("Close", DoubleType(), nullable=True),
        StructField("Volume", DoubleType(), nullable=True),
    ]
)
