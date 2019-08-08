# encoding: utf-8

"""

@Author: wanghuagang
@Contact: huagang517@126.com
@Project: StudySpark
@File:  d3
@Date: 2019/6/28 下午4:21
@Description:

"""
import os

# os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# avro_jar_file = '/home/kevin/Downloads/spark-avro_2.11-4.0.0.jar'
# avro_jar_file = 'hdfs://m1.node.hadoop:8020/tmp/spark-avro_2.11-4.0.0.jar'
avro_jar_file = '/tmp/test/spark-avro_2.11-4.0.0.jar'

# .set("spark.jars", avro_jar_file)

conf = SparkConf().setAppName('aaa').setMaster('yarn').set("spark.pyspark.python", "python3")
sc = SparkContext(conf=conf)

# print(sc)
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", avro_jar_file) \
    .getOrCreate()
print(spark)

# avro_file = '../data/001.snappy.avro'
# avro_file = '/tmp/episodes.avro'
# avro_file = '/home/kevin/workspaces/develop/python/pyspark/StudySpark/data/users.avro'
# df = spark.read.format("com.databricks.spark.avro").load(avro_file)


avro_file = 'hdfs://m1.node.hadoop:8020/tmp/whg/shunqiwang/year=2018/month=11/day=18/20181118-00c109c8-963b-11e9-a991-1c1b0dcc5950.avro'
schema_name = 'crawlers_shunqiwang'
df = spark.read.format("com.databricks.spark.avro").load(avro_file)

# df = df.limit(50).collect()

contacts = df.select(df.contacts)

types = []

with open('/tmp/types', 'w+', encoding='utf-8') as f:
    f.write('0000000000000000')


def extract_phone(data):
    # with open('/tmp/types', 'w+', encoding='utf-8') as f:
    #     f.write('0000000000000000')
    return data


contacts = contacts.rdd.map(lambda x: extract_phone(x)).collect()


with open('/tmp/types', 'w', encoding='utf-8') as f:
    for i in contacts:
        f.write(str(i) + '\n')


# print(df.schema)
# print(df.show())
