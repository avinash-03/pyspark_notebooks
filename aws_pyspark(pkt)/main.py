import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    my_list = [1, 2, 3]
    spark = SparkSession.builder.appName('my name').enableHiveSupport().getOrCreate()
    df = spark.createDataFrame(my_list, IntegerType())
    df.show()
    df.createOrReplaceTempView("temptable1")
    spark.sql("create table as select * from temptable1")
    print('ok')

if __name__ == "__main__":
    print_hi('avi')