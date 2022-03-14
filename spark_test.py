import pyspark
import pandas as pd
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Test').getOrCreate()

gz = spark.read.json("/Users/rafsanbhuiyan/Documents/GitHub/sayari_sparttest_rafsan_bhuiyan/gbr.jsonl")

print(gz.show())

gz.printSchema()


