# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,explode
import pyspark.sql.functions as f 


# %%
# Extract

spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()


# %%
spark

# %%
df = spark.read.text('data/WordData1.txt')

# %%
print(df.show())

# %%
# transformation
df2 = df.withColumn('splitedData',f.split('value',' '))
df3 = df2.withColumn('words',explode('splitedData'))
word_df = df3.select('words')
word_df.show()

# %%
word_count = word_df.groupBy('words').count()

# %%


# %%
word_count.show()

# %%
# LOAD 
driver = "com.mysql.jdbc.Driver"
url = 'jdbc:mysql://database-2.cjbysn8oqrq1.us-east-1.rds.amazonaws.com/'
table = 'avinash_schema.wordcount'
user='admin'
password='ad789min'

word_count.write.mode('append').format("jdbc").option("url",url).option("driver",driver)\
    .option("dbtable",table).option("mode","append").option("user",user)\
        .option("password",password).save()

# %%
# LOAD 
driver1 = "com.mysql.jdbc.Driver"
url1 = 'jdbc:mysql://localhost:3306/'
table1 = 'study.wordcount'
user1='root'
password1='root'

word_count.write.mode('append').format('jdbc').option("url",url1)\
    .option("driver",driver1)\
    .option('dbtable',table1).option('mode','append').option('user',user1)\
        .option('password',password1).save()

# %%
# from pyspark.sql import SparkSession

# spark = SparkSession\
#     .builder\
#     .appName("Word Count")\
#     .getOrCreate()

dataframe_mysql = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:mysql://localhost/exam")\
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("dbtable", "books").option("user", "root")\
    .option("password", "root").load()

print(dataframe_mysql.columns)

# %%
dataframe_mysql.show()

# %%



