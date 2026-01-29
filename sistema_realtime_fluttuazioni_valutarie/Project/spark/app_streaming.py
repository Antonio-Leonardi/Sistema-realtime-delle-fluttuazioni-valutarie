from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, concat, lit,
    max_by, abs as abs_col, broadcast
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)
import requests
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FX-STREAM")

spark = SparkSession.builder \
    .appName("FX Streaming -> Elasticsearch Bulk") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("base_currency", StringType()),
    StructField("target_currency", StringType()),
    StructField("rate", DoubleType()),
    StructField("@timestamp", TimestampType()),
    StructField("ingest_time", TimestampType())
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "fx_rates") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

fx_df = parsed_df \
    .withColumnRenamed("@timestamp", "event_time") \
    .withColumn(
        "currency_pair",
        concat(col("base_currency"), lit("/"), col("target_currency"))
    )

currency_info = spark.read.csv(
    "/opt/spark-app/data.csv",
    header=True,
    inferSchema=True
)

currency_info = broadcast(currency_info)

enriched_df = fx_df.join(
    currency_info,
    fx_df.target_currency == currency_info.currency,
    "left"
)

enriched_df = enriched_df.filter(col("category") != "obsolete")

windowed_df = enriched_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("currency_pair"),
        col("name"),
        col("category"),
        col("region"),
        col("volatility")
    ) \
    .agg(
        avg("rate").alias("avg_rate"),
        max_by(col("rate"), col("event_time")).alias("last_rate")
    )

result_df = windowed_df.withColumn(
    "rate_change_pct",
    ((col("last_rate") - col("avg_rate")) / col("avg_rate")) * 100
)

THRESHOLD = 0.05 

interesting_df = result_df.filter(
    abs_col(col("rate_change_pct")) > THRESHOLD
)

ES_URL = "http://elasticsearch:9200/fx-alerts/_bulk"
ES_HEADERS = {"Content-Type": "application/x-ndjson"}

def send_to_elasticsearch(batch_df, batch_id):
    logger.info(f"foreachBatch START | batch_id={batch_id}")

    count = batch_df.count()
    logger.info(f"Records in batch: {count}")

    if count == 0:
        logger.info("Empty batch -> skip send to Elasticsearch")
        return

    rows = batch_df.collect()
    bulk_payload = ""

    for row in rows:
        doc = row.asDict(recursive=True)

        # Reinserisce il campo @timestamp per Kibana
        if "event_time" in doc:
            doc["@timestamp"] = doc["event_time"]

        # Azione di index per il Bulk API
        bulk_payload += json.dumps({
            "index": {"_index": "fx-alerts"}
        }) + "\n"

        # Serializzazione robusta: converte tutti i datetime in ISO8601
        bulk_payload += json.dumps(
            doc,
            default=lambda o: o.isoformat() if hasattr(o, "isoformat") else str(o)
        ) + "\n"

    response = requests.post(
        ES_URL,
        headers=ES_HEADERS,
        data=bulk_payload
    )

    if response.status_code >= 300:
        logger.error(f"Elasticsearch error: {response.text}")
    else:
        logger.info(f"Sent {count} documents to Elasticsearch")

console_query = interesting_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .start()

es_query = interesting_df.writeStream \
    .foreachBatch(send_to_elasticsearch) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/fx") \
    .start()

spark.streams.awaitAnyTermination()