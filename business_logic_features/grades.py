from pyspark.sql import functions as F


def get_grade(df):
    # Add new column/feature to the dataframe to assign a grade
    df_with_feature = df.withColumn("grade",
                                    F.when(df["score"] >= 90, "A")
                                    .when((df["score"] >= 75) & (df["score"] < 90), "B")
                                    .when((df["score"] >= 60) & (df["score"] < 75), "C")
                                    .otherwise("D"))
    return df_with_feature
