# encoding: utf-8

"""

@Author: wanghuagang
@Contact: huagang517@126.com
@Project: StudySpark
@File:  shunqiwang
@Date: 2019/7/3 上午11:54
@Description:

"""
import os
import re
from typing import List, Dict

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

avro_jar_file = 'spark-avro_2.11-4.0.0.jar'

spark = SparkSession.builder \
    .appName('shunqiwang') \
    .config('spark.jars', os.path.join(BASE_DIR, 'lib', avro_jar_file)) \
    .config('spark.pyspark.python', 'python3') \
    .getOrCreate()

# df = spark.read.format('com.databricks.spark.avro').load('../data/shunqiwang.avro')

CREATE_SQL = """CREATE EXTERNAL TABLE IF NOT EXISTS shunqiwang
PARTITIONED BY (year int, month int, day int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/tmp/whg/shunqiwang/'
TBLPROPERTIES ('avro.schema.literal'='{
    "type": "record",
    "name": "shunqiwang_company_info",
    "fields": [
        {"name": "url", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "abouts", "type": ["null", "string"]},
        {"name": "products", "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "name": "products",
                        "type": "record",
                        "fields": [
                            {"name": "desc", "type": ["null", "string"], "doc": "产品描述"},
                            {"name": "price","type": ["null", "string"], "doc": "价格"},
                            {"name": "time", "type": ["null", "string"], "doc": "时间"},
                            {"name": "title", "type": "string", "doc": "标题"},
                            {"name": "url", "type": "string", "doc": "链接"}
                        ]
                    }
                }
            ]
        },
        {"name": "contacts", "type": [
                "null",
                {
                    "type": "map",
                    "values": "string"
                }
            ]
        },
        {"name": "business_info", "type": [
                "null",
                {
                    "type": "map",
                    "values": "string"
                }
            ]
        },
        {"name": "shareholders","type": [
                "null",
                {
                    "name": "shareholders",
                    "type": "record",
                    "fields": [
                        {"name": "investment_proportion", "type": ["null", "string"], "doc": "出资比例"},
                        {"name": "capital_contribution", "type": ["null", "string"], "doc": "出资额"},
                        {"name": "type", "type": ["null", "string"], "doc": "类型"},
                        {"name": "name","type": "string", "doc": "股东名字"}
                    ]
                }
            ]
        },
        {"name": "business_info_change", "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "name": "business_info",
                        "type": "record",
                        "fields": [
                            {"name": "before","type": ["null", "string"], "doc": "变更前"},
                            {"name": "after","type": ["null", "string"], "doc": "变更后"},
                            {"name": "item","type": ["null", "string"], "doc": "变更项目"},
                            {"name": "time","type": ["null", "string"], "doc": "时间"}
                        ]
                    }
                }
            ]
        },
        {"name": "position", "type": [
                "null",
                {

                    "type": "array",
                    "items": {
                        "name": "position",
                        "type": "record",
                        "fields": [
                            {"name": "name", "type": "string", "doc": "名字"},
                            {"name": "job", "type": ["null", "string"], "doc": "职务"}
                        ]
                    }
                }
            ]
        },
        {"name": "lawsuit", "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "name": "lawsuit",
                        "type": "record",
                        "fields": [
                            {"name": "name", "type": "string", "doc": "文书名称"},
                            {"name": "time", "type": "string", "doc": "日期"},
                            {"name": "num", "type": "string", "doc": "编号"}
                        ]
                    }
                }
            ]
        }
    ]
}')"""

spark.sql(CREATE_SQL)
spark.sql('MSCK REPAIR TABLE shunqiwang')

df = spark.sql('SELECT * FROM shunqiwang')
df.show()


def print_res(res, desc=None):
    print(f'\n====================================')
    if desc:
        print(desc)
    print(res)
    print(f'====================================\n')


# print_res(df.count())
#
contacts_df = df.select(df.contacts)
# print_res(contacts_df.count(), 'contacts_df count:')
# contacts_df.show()


def is_phone_by_length(data, phone_length):
    """
    根据长度判断
    """
    if len(data) == phone_length:
        return True
    else:
        return False


def is_phone_by_keywords(data, keywords=[]):
    """
    通过 key 中时候包含关键字判断
    """
    keywords = keywords or ['电话', '手机']
    for keyword in keywords:
        if keyword in data:
            return True
    return False


def is_phone_by_regular(data, regular_expression):
    """
    通过正则表达式判断
    """
    match = re.findall(regular_expression, data)
    if match:
        return True
    return False


example_mobile_phone = '13000000000'
example_fixed_phone = '(0795)'

phone_pattern = re.compile(r'^\w*?(?:\(\d{2,4}\)\S{6,})|\d{3,4}(?:(-\S{6,})|\d{4,})')


def extract(data: List[Dict]):
    row = {}
    for item in data:
        for k, v in item.items():
            row.update({k: v})
    return Row(**row)


def extract_contacts(data: List[Dict]):
    row = {'手机': None, '电话': None, 'other': None}
    # row = {}
    for item in data:
        for k, v in item.items():
            if is_phone_by_keywords(k) and v != '未提供':
                len_v = len(v)
                if len_v == len(example_mobile_phone):
                    row.update({'手机': v})
                elif len_v > len(example_fixed_phone):
                    row.update({'电话': v})
                else:
                    match = phone_pattern.match(v)
                    if match:
                        row.update({'other': v})
    return Row(**row)


phone_schema = StructType([
    StructField('other', StringType(), True),
    StructField('手机', StringType(), True),
    StructField('电话', StringType(), True),
])

email_pattern = re.compile(r'^\w*?@\w+?\.\w+')


def extract_email(data: List[Dict]):
    row = {'email': None, 'other': None}
    for item in data:
        for k, v in item.items():
            match = email_pattern.match(v)
            if match:
                row.update({'email': v})
            # if '电子邮件' == k and v != '未提供':
            #     row.update({'email': v})
            # else:

    return Row(**row)


email_schema = StructType([
    StructField('email', StringType(), True),
    StructField('other', StringType(), True),
])
#
extract_rdd = contacts_df.rdd.map(lambda x: extract_email(x)).collect()
extract_df = spark.createDataFrame(extract_rdd, schema=email_schema).dropna('all')

print_res(extract_df.count(), 'email count:')

extract_df.show(20)
