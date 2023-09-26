import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.testing.utils import assertDataFrameEqual
from src.transforms_spark import remove_non_word_characters

spark = SparkSession.builder.getOrCreate()

def test_remove_non_word_characters():
    # Dirty rows
    dirty_rows = [
          ("jo&&se7",),
          ("**li**",),
          ("#::luisa",),
          (None,)
      ]
    source_df = spark.createDataFrame(dirty_rows, ["name"])

    # Cleaned rows using function
    clean_df = source_df.withColumn(
        "clean_name",
        remove_non_word_characters(F.col("name"))
    )

    # Expected output, should be identical to clean_df
    expected_data = [
          ("jo&&se7", "jose"),
          ("**li**", "li"),
          ("#::luisa", "luisa"),
          (None, None)
      ]
    expected_df = spark.createDataFrame(expected_data, ["name", "clean_name"])

    assertDataFrameEqual(clean_df, expected_df)