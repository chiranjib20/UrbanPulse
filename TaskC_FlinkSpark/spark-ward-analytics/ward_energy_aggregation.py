"""
UrbanPulse - Task C, Problem 10: Ward Energy Aggregation
==========================================================
Spark Structured Streaming job reading urbanpulse.smart_meters and computing,
per ward_id, per 15-minute TUMBLING window:
    total_kwh_consumed, avg_power_factor, peak_voltage

A 45-minute watermark is applied on event time (per the grading rubric's late
-data allowance) - meter readings can legitimately arrive up to 45 minutes
late (e.g. a meter buffering locally during a brief network drop) and will
still be folded into the correct window; anything later than that is dropped
so window state doesn't grow unbounded.

Output is written to BOTH:
  1. Kafka topic ward_energy_summary (for the live dashboard)
  2. A Parquet dataset partitioned by ward_id and date (for historical trend
     analysis / the batch layer's councillor reports)
using foreachBatch so a single micro-batch's result can fan out to two sinks.

Run:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      ward_energy_aggregation.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as _sum, avg, max as _max, to_date, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9095"
SOURCE_TOPIC = "urbanpulse.smart_meters"
SINK_TOPIC = "ward_energy_summary"
PARQUET_PATH = "/data/urbanpulse/ward_energy_summary_parquet"
CHECKPOINT_PATH = "/data/urbanpulse/checkpoints/ward_energy_aggregation"

meter_schema = StructType([
    StructField("meter_id", StringType()),
    StructField("ward_id", StringType()),
    StructField("kwh_reading", DoubleType()),
    StructField("voltage", DoubleType()),
    StructField("power_factor", DoubleType()),
    StructField("timestamp", StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("UrbanPulse-WardEnergyAggregation")
        .config("spark.sql.shuffle.partitions", "10")  # matches smart_meters partition count
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    meters = (
        raw.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), meter_schema).alias("m"))
        .select("m.*")
        .withColumn("event_time", col("timestamp").cast("timestamp"))
        .filter(col("kwh_reading").isNotNull() & col("voltage").isNotNull())
    )

    ward_summary = (
        meters
        .withWatermark("event_time", "45 minutes")   # tolerate late-arriving meter data
        .groupBy(
            window(col("event_time"), "15 minutes"),  # 15-minute tumbling window
            col("ward_id"),
        )
        .agg(
            _sum("kwh_reading").alias("total_kwh_consumed"),
            avg("power_factor").alias("avg_power_factor"),
            _max("voltage").alias("peak_voltage"),
        )
        .select(
            col("ward_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_kwh_consumed"),
            col("avg_power_factor"),
            col("peak_voltage"),
        )
    )

    def write_batch(batch_df, batch_id: int):
        batch_df.persist()

        # Sink 1: Kafka, for the live ops dashboard.
        (
            batch_df
            .selectExpr("ward_id AS key", "to_json(struct(*)) AS value")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("topic", SINK_TOPIC)
            .save()
        )

        # Sink 2: Parquet, partitioned by ward_id and date, for historical trends.
        (
            batch_df
            .withColumn("date", to_date(col("window_start")))
            .write
            .mode("append")
            .partitionBy("ward_id", "date")
            .parquet(PARQUET_PATH)
        )

        batch_df.unpersist()

    query = (
        ward_summary.writeStream
        .foreachBatch(write_batch)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="1 minute")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
