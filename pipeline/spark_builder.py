from pyspark.sql import SparkSession

builder = (SparkSession.builder
           .appName("DeltaLakeLocal")
           .master("local[*]")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
