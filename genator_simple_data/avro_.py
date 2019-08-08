# encoding: utf-8

"""

@Author: wanghuagang
@Contact: huagang517@126.com
@Project: StudySpark
@File:  avro
@Date: 2019/6/28 下午4:23
@Description:

"""

import logging
from datetime import datetime
import avro
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


tmp_file = '/tmp/whg/20190617172619-00c2e005-2c3d-45bb-bdd5-03ff37147e42.avro'

schema = {"namespace": "example.avro",
          "type": "record",
          "name": "User",
          "fields": [
              {"name": "name", "type": "string"},
              {"name": "favorite_number", "type": ["int", "null"]},
              {"name": "favorite_color", "type": ["string", "null"]}
          ]}

schema = avro.schema.SchemaFromJSONData(schema)


data = [
{"name": "Alyssa", "favorite_number": 256},
{"name": "Ben", "favorite_number": 7, "favorite_color": "red"},
]

writer = DataFileWriter(open("../data/users.avro", "wb"), DatumWriter(), schema)
for d in data:
    writer.append(d)
writer.close()

