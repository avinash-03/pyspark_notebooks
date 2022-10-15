from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit, col

if __name__ == "__main__":
    # spark session
    spark = SparkSession.builder.appName("data move").getOrCreate()

    # read data from csv 
    print("reading data from csv")
    df = spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .option("mode","FAILFAST")\
        .load("./data/flight-data/csv/2010-summary.csv")

    df2 = df.withColumn("date",lit(current_date()))


    driver = "com.mysql.jdbc.Driver"
    url = "jdbc:mysql://localhost"
    username= 'root'
    password = 'root'
    dbtable='pyspark.flightdate'


    # savinng to mysql
    print("saving to mysql...")
    df2.write.format("jdbc")\
        .option("driver",driver)\
        .option("url",url)\
        .option("user",username)\
        .option("password",password)\
        .option("dbtable",dbtable)\
        .save()

    print(f"succesfulley saved in mysql with name {dbtable}")
