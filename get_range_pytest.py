import pytest
from pyspark.sql import SparkSession
from business_logic_features.grades import get_grade

@pytest.fixture(scope="session")
def spark_session(request):
    spark = SparkSession.builder.appName("spark_unit_tests").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    request.addfinalizer(lambda: spark.stop())
    return spark


def test_grade(spark_session):
    # Create DataFrame - Get test data
    df_expected = spark_session.read.option("header", True).csv("test_data/test_data.csv")
    # df_expected.show()
    # Calculate Features
    df_actual = get_grade(df_expected).drop('expected_grade')
    # df_actual.show()
    # Ensure both dataframes have same number of columns
    assert df_expected.collect() == df_actual.collect()