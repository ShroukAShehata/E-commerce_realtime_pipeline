from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_date
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("kafka_to_bronze") \
    .getOrCreate()

schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("session_id", StringType()) \
    .add("event_type", StringType()) \
    .add("product_id", IntegerType()) \
    .add("ts", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream_raw") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_str")

parsed = json_df \
    .select(from_json(col("json_str"), schema).alias("d")) \
    .select("d.*") \
    .withColumn("ingest_ts", current_timestamp()) \
    .withColumn("ingest_date", to_date(col("ingest_ts")))

query = parsed.writeStream \
    .format("parquet") \
    .option(
        "path",
        "gs://ecommerce-datalake/bronze/clickstream/"
    ) \
    .option(
        "checkpointLocation",
        "gs://ecommerce-datalake/checkpoints/clickstream/"
    ) \
    .partitionBy("ingest_date") \
    .outputMode("append") \
    .start()

query.awaitTermination()
