from pyspark import SparkConf,SparkContext

conf = SparkConf().setAppName('read_file_text')

# command 
sc = SparkContext.getOrCreate(conf=conf)

text = sc.textFile('sample.txt')

print(text.collect())

