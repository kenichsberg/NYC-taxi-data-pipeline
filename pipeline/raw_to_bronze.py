from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from spark_builder import builder

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_taxi_trips = spark.read.load("pipeline/data/yellow_taxi_jan_25_2018")
df_taxi_trips.printSchema()
df_taxi_trips.show()

(df_taxi_trips.write
 .format("delta")
 .mode("overwrite")
 .save("/tmp/delta/bronze/trips"))


df_taxi_zones = (spark.read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .csv("pipeline/data/taxi_zone_lookup.csv"))

df_taxi_zones.show()

(df_taxi_zones
 .orderBy(F.col("LocationID").desc_nulls_first())
 .show())

(df_taxi_zones.write
 .format("delta")
 .mode("overwrite")
 .save("/tmp/delta/zones"))


print(df_taxi_trips.count())
