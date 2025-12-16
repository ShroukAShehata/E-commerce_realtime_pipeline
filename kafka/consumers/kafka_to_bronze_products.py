from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("products_cdc_to_gcs") \
    .getOrCreate()


after_schema = StructType() \
    .add("product_id", IntegerType()) \
    .add("name", StringType()) \
    .add("category", StringType()) \
    .add("price", DoubleType())

value_schema = StructType() \
    .add("after", after_schema) \
    .add("op", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mongo.ecom.products") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_str")

parsed = json_df \
    .select(from_json(col("json_str"), value_schema).alias("d")) \
    .select("d.after.*", "d.op") \
    .filter(col("op").isin("c", "u")) \
    .withColumn("ingest_ts", current_timestamp())

query = parsed.writeStream \
    .format("parquet") \
    .option(
        "path",
        "gs://ecommerce-datalake/bronze/products_cdc/"
    ) \
    .option(
        "checkpointLocation",
        "gs://ecommerce-datalake/checkpoints/products_cdc/"
    ) \
    .outputMode("append") \
    .start()

query.awaitTermination()
