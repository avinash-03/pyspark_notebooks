from pyspark.sql import SparkSession


if __name__ =="__main__":
    spark = SparkSession.builder.appName("wordcount").getOrCreate()
    print(spark.range(5000).where("id>500").selectExpr("sum(ID)").collect())

