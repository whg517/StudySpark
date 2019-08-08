from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

print(spark)

conf = SparkConf().setAppName('aaa').setMaster('local')
sc = SparkContext(conf=conf)
print(sc)
