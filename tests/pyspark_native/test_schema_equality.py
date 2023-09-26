import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertSchemaEqual

spark = SparkSession.builder.getOrCreate()

def test_schema_mismatch():
    # DF with numeric and string columns
    data1 = [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["num", "letter"])
    
    # DF with only numeric columns
    data2 = [
        (1, 88.8),
        (2, 99.9),
        (3, 1000.1),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "double"])
    
    # Compare them
    assertSchemaEqual(df1.schema, df2.schema)