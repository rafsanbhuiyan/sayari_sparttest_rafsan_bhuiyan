from ast import alias
from unicodedata import name
from numpy import inner
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as sf
from pyspark.sql.functions import *

sc = SparkContext(master="local[*]", appName = "data_json")

spark = SparkSession.builder.appName('data_json').getOrCreate()

#Reading in JSON data
normal_gbr_df = spark.read.json("/Users/rafsanbhuiyan/Documents/GitHub/sayari_sparttest_rafsan_bhuiyan/gbr.jsonl")

normal_ofac_df = spark.read.json("/Users/rafsanbhuiyan/Documents/GitHub/sayari_sparttest_rafsan_bhuiyan/ofac.jsonl")

################ NORMALIZING GBR.JSONL dataset
#Flatten nested JSON
gbr = normal_gbr_df.withColumn("country",normal_gbr_df.addresses.country)
gbr = gbr.withColumn("postal_code",normal_gbr_df.addresses.postal_code)

#explode
explode_gbr = gbr.withColumn("reported_DOB", explode("reported_dates_of_birth"))


explode_gbr = explode_gbr.withColumn("id_number", explode("id_numbers")).withColumn("nationality", explode("nationality"))

explode_gbr = explode_gbr.withColumn("id_comment", explode_gbr.id_number.comment).withColumn("id_value",explode_gbr.id_number.value).withColumn("country", explode("country")).withColumn("postal_code", explode("postal_code"))

gbr_df = explode_gbr.select("id","id_comment","id_value", "name", "nationality", "place_of_birth", "position", "type",  "country", "postal_code", to_date("reported_DOB", 'dd/MM/yyyy').alias("reported_DOB"))

gbr_df = gbr_df.withColumnRenamed("id", "uk_id")

gbr_df.printSchema()

gbr_df.show()

################ NORMALIZING OFAC.JSONL dataset
#normal_ofac_df.printSchema()
#normal_ofac_df.show(truncate=60)

ofac_df = normal_ofac_df.select("id", "id_numbers", "name", "nationality", "place_of_birth", "position", "reported_dates_of_birth","type", "aliases", "addresses")


#FLATTEN NESTED JSON
ofac_df = ofac_df.withColumn("country",explode(ofac_df.addresses.country))
ofac_df = ofac_df.withColumn("postal_code",explode(ofac_df.addresses.postal_code))
ofac_df = ofac_df.withColumn("aliases",explode(ofac_df.aliases.type))
ofac_df = ofac_df.drop(ofac_df.addresses)
ofac_df = ofac_df.withColumn("id_comment", explode(ofac_df.id_numbers.comment)).withColumn("id_value", explode(ofac_df.id_numbers.value))
ofac_df = ofac_df.drop(ofac_df.id_numbers)
ofac_df = ofac_df.withColumn("nationality", explode(ofac_df.nationality)). withColumn("reported_DOB", explode(ofac_df.reported_dates_of_birth))
ofac_df = ofac_df.drop(ofac_df.reported_dates_of_birth)
#gbr_df = explode_gbr.select("id","id_comment","id_value", "name", "nationality", "place_of_birth", "position", "type",  "country", "postal_code", to_date("reported_DOB", 'dd/MM/yyyy').alias("reported_DOB"))
ofac_df = ofac_df.select("id", "id_comment", "id_value", "name", "nationality", "place_of_birth", "position", "type", "aliases", "country", "postal_code", to_date("reported_DOB", 'dd MMM yyyy').alias("reported_DOB"))
ofac_df = ofac_df.withColumnRenamed("id","ofac_id")
ofac_df.show(truncate=40)
ofac_df.printSchema()

#JOINING SPARK DATAFRAMES
output_data = ofac_df.join(gbr_df, on=["name", "nationality"], how="inner")

output_data = output_data.select(output_data.ofac_id, output_data.uk_id, output_data.name, output_data.id_comment, output_data.id_value, output_data.nationality, output_data.place_of_birth, output_data.position, output_data.type, output_data.type, output_data.country, output_data.postal_code, output_data.reported_DOB)

output_data.show()
##############CODE ARCHIVES###################
#gz = gz.withColumn("reported_DOB", concat_ws(", ", normal_gbr_df.reported_dates_of_birth))

#gz_join = gz.join(explode_gz, on = "id", how = "inner")

#gz = normal_gbr_df.drop(normal_gbr_df.reported_dates_of_birth)

#gbr_df.show()

#gbr_df.select("reported_DOB").show(truncate=False)

#gbr_df.printSchema()

#gbr_df = gbr_df.select("reported_DOB", date_format("reported_DOB", "MM/dd/yyyy").alias("reported_DOB")).show()

#normal_ofac_df.show()

#normal_ofac_df.printSchema()

#gz.printSchema()



