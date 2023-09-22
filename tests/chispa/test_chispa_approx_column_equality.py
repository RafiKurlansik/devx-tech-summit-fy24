import pytest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from chispa import *
from chispa.dataframe_comparer import *
from covid_analysis.transforms_spark import remove_non_word_characters

spark = SparkSession.builder.getOrCreate()

# Pandas, a popular DataFrame library, does consider NaN values to be equal by default. chispa requires you to set a flag to consider two NaN values to be equal.
# assert_df_equality(df1, df2, allow_nan_equality=True)

# We can check if columns are approximately equal, which is especially useful for floating number comparisons. Here's a test that creates a DataFrame with two floating point columns and verifies that the columns are approximately equal. In this example, values are considered approximately equal if the difference is less than 0.1.

def test_approx_col_equality_same():
    data = [
        (1.1, 1.1),
        (2.2, 2.15),
        (3.3, 3.37),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["num1", "num2"])
    assert_approx_column_equality(df, "num1", "num2", 0.1)

def test_approx_col_equality_different():
    data = [
        (1.1, 1.1),
        (2.2, 2.15),
        (3.3, 5.0),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["num1", "num2"])
    assert_approx_column_equality(df, "num1", "num2", 0.1)