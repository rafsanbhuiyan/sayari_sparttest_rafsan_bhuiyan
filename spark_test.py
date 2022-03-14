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

#normal_gbr_df.show()



#normal_gbr_df.printSchema()



gz = normal_gbr_df.withColumn("country",normal_gbr_df.addresses.country)
gz = gz.withColumn("postal_code",normal_gbr_df.addresses.postal_code)
#gz = gz.withColumn("reported_DOB", concat_ws(", ", normal_gbr_df.reported_dates_of_birth))

#explode
explode_gz = gz.withColumn("reported_DOB", explode("reported_dates_of_birth"))


explode_gz = explode_gz.withColumn("id_number", explode("id_numbers")).withColumn("nationality", explode("nationality"))

explode_gz = explode_gz.withColumn("comment", explode_gz.id_number.comment).withColumn("value",explode_gz.id_number.value).withColumn("country", explode("country")).withColumn("postal_code", explode("postal_code"))


gbr_df = explode_gz.select("id","comment","value", "name", "nationality", "place_of_birth", "position", "type", to_date("reported_DOB", 'dd/MM/yyyy').alias("reported_DOB"), "country", "postal_code")

gbr_df.printSchema()

gbr_df.show(truncate = 50)

#gz_join = gz.join(explode_gz, on = "id", how = "inner")


#gz = normal_gbr_df.drop(normal_gbr_df.reported_dates_of_birth)



#gbr_df.show()

#gbr_df.select("reported_DOB").show(truncate=False)

#gbr_df.printSchema()

#gbr_df = gbr_df.select("reported_DOB", date_format("reported_DOB", "MM/dd/yyyy").alias("reported_DOB")).show()

#normal_ofac_df.show()

#normal_ofac_df.printSchema()

#gz.printSchema()



