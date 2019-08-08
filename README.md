### spark-submit

```shell

spark-submit \
    --jars /tmp/test/spark-avro_2.11-4.0.0.jar \    # 以来 jar 传入
    --conf spark.pyspark.python=python3  \  # 指定 driver and worker python
    pyspark_demo/read_avro.py

```

指定 Python 解释器

`PYSPARK_PYTHON=python3`

增加 Hive 支持

`--conf spark.sql.catalogImplementation=hive`

增大 driver 端内存

`--conf spark.driver.maxResultSize 3G`

## Example

### shunqiwang

```bash

spark-submit \
    --jars ./lib/spark-avro_2.11-4.0.0.jar \
    --driver-memory 10G \
    --driver-cores 4 \
    --num-executors 10\
    --executor-memory 500M \
    --executor-cores 2 \
    --conf spark.pyspark.python=python3 \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.driver.maxResultSize=5G \
    pyspark_demo/shunqiwang.py
```

手机号码 19005168
email count: 6280513