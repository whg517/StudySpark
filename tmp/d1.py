import json
import os

import requests
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

print(json.dumps(dict(sorted(os.environ.items())), indent=4))

#
# conf = SparkConf().setAppName('aaa').setMaster('local')
# sc = SparkContext(conf=conf)
#
# print(sc)

#
# spark = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()
#
# print(spark)
# spark.read.format('avro').load()
# from requests_kerberos import HTTPKerberosAuth
#
# r = requests.get('http://m2.node.hadoop:7788/api/v1/schemaregistry/schemas/crawlers_shunqiwang/versions/latest',
#                  auth=HTTPKerberosAuth())
#
# schema_txt = json.loads(r.text).get('schemaText')
# print(json.loads(schema_txt))
