# spark_kafka_foreachbatch_data.py
import os
import io
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, DoubleType
from pyspark.sql.functions import col, current_timestamp, lit, udf
import fastavro
from fastavro.schema import parse_schema

# Ensure local "data" directory exists
os.makedirs("data", exist_ok=True)

# Kafka bootstrap server (use env var for Docker, default to localhost for local dev)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Avro schema (must match the producer schema)
AVRO_SCHEMA = parse_schema({
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "user", "type": "string"},
        {"name": "event", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "ts", "type": "long"}
    ]
})

spark = (
    SparkSession.builder
    .appName("kafka-foreachBatch-data")
    .getOrCreate()
)

# ----- Paths -----
checkpoint  = "./checkpoint_hsutest"
output_path = "./data"   # local directory for Parquet files

# 1) Kafka source
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "hsudemo")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 100)  # ~100 records per micro-batch
    .load()
)

# 2) Parse Avro payload using UDFs
def deserialize_avro_field(field_name):
    """Create a UDF to extract a specific field from Avro binary data."""
    def extract_field(avro_bytes):
        if avro_bytes is None:
            return None
        buffer = io.BytesIO(avro_bytes)
        record = fastavro.schemaless_reader(buffer, AVRO_SCHEMA)
        return record.get(field_name)
    return extract_field

# Register UDFs for each field
extract_user = udf(deserialize_avro_field("user"), StringType())
extract_event = udf(deserialize_avro_field("event"), StringType())
extract_amount = udf(deserialize_avro_field("amount"), DoubleType())
extract_ts = udf(deserialize_avro_field("ts"), LongType())

parsed = raw.select(
    extract_user(col("value")).alias("user"),
    extract_event(col("value")).alias("event"),
    extract_amount(col("value")).alias("amount"),
    extract_ts(col("value")).alias("ts"),
    col("topic"), col("partition"), col("offset"), col("timestamp").alias("kafka_ts")
)

# 3) foreachBatch writer
def write_batch(batch_df, batch_id: int):
    out = (batch_df
           .withColumn("batch_id", lit(batch_id))
           .withColumn("ingest_ts", current_timestamp()))

    (out.write
        .mode("append")
        .partitionBy("batch_id")   # one folder per batch
        .parquet(output_path))

# 4) Start query
query = (
    parsed.writeStream
    .foreachBatch(write_batch)
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .start()
)

query.awaitTermination()