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
        (1, "d"),
        (2, "e"),
        (3, "f"),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "letter"])
    
    # Compare them
    assertSchemaEqual(s1, s2)