from pyspark.sql.functions import year, month, dayofmonth


def transform_df(df):
    trans_df = df.withColumn("year", year('created_at')) \
        .withColumn("month", month('created_at')) \
        .withColumn("day", dayofmonth('created_at')) \
        .select("repo.*", "year", 'month', 'day')
    return trans_df

