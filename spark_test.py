from sqlite3 import DateFromTicks
import pyspark
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as sf
from pyspark.sql.functions import *




sc = SparkContext(master="local[*]", appName = "data_json")

spark = SparkSession.builder.appName('data_json').getOrCreate()

normal_gbr_df = spark.read.json("/Users/rafsanbhuiyan/Documents/GitHub/sayari_sparttest_rafsan_bhuiyan/gbr.jsonl")

normal_ofac_df = spark.read.json("/Users/rafsanbhuiyan/Documents/GitHub/sayari_sparttest_rafsan_bhuiyan/ofac.jsonl")

print(spark.catalog.listTables())

normal_gbr_df.show()

#normal_ofac_df.show()

normal_gbr_df.printSchema()

#normal_ofac_df.printSchema()

gz = normal_gbr_df.withColumn("country",normal_gbr_df.addresses.country)
gz = normal_gbr_df.withColumn("postal_code",normal_gbr_df.addresses.postal_code)

gz.show()



#gz.printSchema()



