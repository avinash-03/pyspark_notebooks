from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('partitiontest').getOrCreate()


def debug(iterator):
    print("elements=", list(iterator))
    

# partition 
numbers = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd = spark.sparkContext.parallelize(numbers)
num_partition = rdd.getNumPartitions()

print('#'*20+str(num_partition)+"#"*10)

rdd.foreachPartition(debug)

print("+"*80)