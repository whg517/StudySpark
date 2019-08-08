# encoding: utf-8

"""

@Author: wanghuagang
@Contact: huagang517@126.com
@Project: StudySpark
@File:  d2
@Date: 2019/6/28 上午10:30
@Description:

"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('aaa').setMaster('local')
sc = SparkContext(conf=conf)

print(sc)
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
print(spark)

json_file = '../data/user.json'

# df = spark.read.format("avro").load(avro_file)
df = spark.read.json(json_file)
print(df.schema)
print(df.show())

print(df.filter(df['age'] >= 18).show())
df.write.json('user_filter.json')


