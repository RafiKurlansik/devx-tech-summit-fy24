import pytest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from chispa import *
from chispa.dataframe_comparer import *

spark = SparkSession.builder.getOrCreate()

# DataFrame equality messages peform schema comparisons before analyzing the actual content of the DataFrames. DataFrames that don't have the same schemas should error out as fast as possible. Let's compare a DataFrame that has a string column an integer column with a DataFrame that has two integer columns to observe the schema mismatch message.
def test_schema_mismatch_message():
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
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "letter"])
    
    # Compare them
    assert_df_equality(df1, df2)