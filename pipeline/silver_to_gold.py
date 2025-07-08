from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from spark_builder import builder

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_taxi_trips_silver = (spark.read
                        .format("delta")
                        .load("/tmp/delta/silver/trips"))

df_taxi_zones = (spark.read
                 .format("delta")
                 .load("/tmp/delta/zones")
                 .drop("service_zone"))

df_taxi_trips_gold = (df_taxi_trips_silver
                      .withColumn(
                      "duration_minute",
                      F.timestamp_diff(
                      "MINUTE",
                      F.col("PUDatetime"),
                      F.col("DODatetime")
                      )
                      )
                      .withColumn(
                      "fare_per_minute",
                      F.try_divide(F.col("fare_amount"), F.col("duration_minute"))
                      )
                      .groupBy("PULocationID", "DOLocationID")
                      .agg(
                      F.count("*").alias("trips_count"),
                      F.sum(F.col("fare_per_minute")).alias("sum")
                      )
                      .withColumn(
                      "fare_per_minute_per_trip",
                      F.try_divide(F.col("sum"), F.col("trips_count"))
                      )
                      .drop("sum")

                      .join(df_taxi_zones, F.col("PULocationID") == F.col("LocationID"))
                      .withColumnRenamed("Borough", "PUBorough")
                      .withColumnRenamed("Zone", "PUZone")
                      .drop("LocationID")
                      .join(df_taxi_zones, F.col("DOLocationID") == F.col("LocationID"))
                      .withColumnRenamed("Borough", "DOBorough")
                      .withColumnRenamed("Zone", "DOZone")
                      .drop("LocationID")
                      .withColumn(
                      "PULocation",
                      F.format_string(
                      "%s (%s)", 
                      F.col("PUBorough"), 
                      F.col("PUZone"))
                      )
                      .withColumn(
                      "DOLocation",
                      F.format_string(
                      "%s (%s)", 
                      F.col("DOBorough"), 
                      F.col("DOZone"))
                      )
                      .drop(
                      "PUBorough",
                      "PUZone",
                      "DOBorough",
                      "DOZone")
                      .orderBy(F.col("fare_per_minute_per_trip").desc_nulls_last()))

df_taxi_trips_gold.show()
df_taxi_trips_gold.orderBy(F.col("trips_count").desc_nulls_last()).show()

(df_taxi_trips_gold.write
 .format("delta")
 .mode("overwrite")
 .save("/tmp/delta/gold/trips"))
