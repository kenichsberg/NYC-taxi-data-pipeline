from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from spark_builder import builder

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_taxi_trips_bronze = (
    spark.read
    .format("delta")
    .load("/tmp/delta/bronze/trips")
)


df_taxi_trips_silver = (df_taxi_trips_bronze
                        #.join(df_taxi_zones, F.col("PULocationID") == F.col("LocationID"))
                        #.withColumnRenamed("Borough", "PUBorough")
                        #.withColumnRenamed("Zone", "PUZone")
                        #.join(df_taxi_zones, F.col("DOLocationID") == F.col("LocationID"))
                        #.withColumnRenamed("Borough", "DOBorough")
                        #.withColumnRenamed("Zone", "DOZone")
                        #.filter(
                        #    (F.col("PUBorough") != "Unknown") 
                        #        & (F.col("PUBorough") != "N/A")
                        #)
                        #.filter(
                        #    (F.col("DOBorough") != "Unknown") 
                        #        & (F.col("DOBorough") != "N/A")
                        #)
                        .filter(
                        (F.col("PULocationID") != 264) 
                        & (F.col("PULocationID") != 265)
                        )
                        .filter(
                        (F.col("DOLocationID") != 264) 
                        & (F.col("DOLocationID") != 265)
                        )
                        .filter(F.col("fare_amount") > 0)
                        .filter(F.col("total_amount") > 0)
                        .filter(F.col("passenger_count") > 0)
                        .drop("VendorID")
                        .withColumnRenamed("tpep_pickup_datetime", "PUDatetime")
                        .withColumnRenamed("tpep_dropoff_datetime", "DODatetime"))

df_taxi_trips_silver.show()

(df_taxi_trips_silver.write
 .format("delta")
 .mode("overwrite")
 .save("/tmp/delta/silver/trips"))

print(df_taxi_trips_silver.count())
