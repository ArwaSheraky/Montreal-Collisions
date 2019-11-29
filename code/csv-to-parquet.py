#!/usr/bin/env python
# coding: utf-8

try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql import SQLContext
    import os

except Exception as e:
    print(e)


spark = SparkSession.builder.master('local[1]').appName("ParquetApp").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

# create sparksession

spark = SparkSession \
    .builder \
    .appName("ParquetApp") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df_csv = spark.read.csv('/script/data/accidents_new.csv', header='true', inferSchema = True)

# Save to HDFS
df_csv.write.save('hdfs://hadoop:8020/cebd1261/accidents.parquet', format='parquet', mode='append')

# Save it locally
# df_csv.write.parquet('/script/data/accidents.parquet/')

# Load parquet file as a Dataframe
parquetFile = sqlContext.read.format('parquet').load('hdfs://hadoop:8020/cebd1261/accidents.parquet/') 

# SQL Example..
parquetFile.createOrReplaceTempView("parquetFile")
filtered = spark.sql("SELECT ID, NB_VICTIMES_TOTAL FROM parquetFile WHERE DATE == '2012-02-02' AND WEEK_DAY == 'JE' LIMIT 20")
filtered.show()

print("DONE! -----------------------------------")
sc.stop()
