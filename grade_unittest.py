import unittest
from pyspark.sql import SparkSession
from business_logic_features.grades import get_grade


class GradeUnitTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("spark_unit_tests").enableHiveSupport().getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def tearDown(self):
        self.spark.stop()

    def test_grade(self):
        # Create DataFrame - Get test data
        df_expected = self.spark.read.option("header", True).csv("test_data/test_data.csv")
        # df_expected.show()
        # Calculate Features
        df_actual = get_grade(df_expected).drop('expected_grade')
        # df_actual.show()
        # Ensure both dataframes have same number of columns
        self.assertEqual(df_expected.collect(), df_actual.collect())


if __name__ == '__main__':
    unittest.main()
