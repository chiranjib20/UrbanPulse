"""
UrbanPulse - Task C, Problem 11: AQI Health Advisory (Streaming SQL)
=======================================================================
(a) 10-minute ROLLING average AQI per zone - implemented as a sliding window
    (slideDuration < windowDuration) rather than a tumbling window, since
    "rolling average" means the average should update continuously as new
    readings arrive, not just every 10 minutes on a fixed boundary. We slide
    every 1 minute.
(b) Joins the rolling result with the static zone_profile table (zone name,
    population, number of schools) - a stream-static join, so Spark
    broadcasts the small static table to every executor.
(c) Filters for rolling_avg_aqi > 150 (Unhealthy) and writes to
    urbanpulse.health_advisories using Update output mode, since this is an
    aggregation query and only changed (zone, window) rows need re-emitting.

The windowed aggregation itself is expressed as a Spark SQL query (per the
assignment's "Streaming SQL query" wording) registered against a temporary
view; the static join and final filter/write use the DataFrame API for
clarity, but are equivalent to the SQL form shown in the docstring below:

    SELECT
        window.start AS window_start, window.end AS window_end,
        zone, AVG(aqi) AS rolling_avg_aqi
    FROM aqi_readings
    GROUP BY window(event_time, '10 minutes', '1 minute'), zone

Run:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      aqi_health_advisory.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, broadcast, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9095"
SOURCE_TOPIC = "urbanpulse.air_quality"
SINK_TOPIC = "urbanpulse.health_advisories"
ZONE_PROFILE_CSV = "zone_profile.csv"
CHECKPOINT_PATH = "/data/urbanpulse/checkpoints/aqi_health_advisory"
UNHEALTHY_THRESHOLD = 150.0

aqi_schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("zone", StringType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("aqi", DoubleType()),
    StructField("sensor_status", StringType()),
    StructField("timestamp", StringType()),
])


def main():
    spark = (
        SparkSession.builder
        .appName("UrbanPulse-AqiHealthAdvisory")
        .config("spark.sql.shuffle.partitions", "3")  # matches air_quality partition count
        .getOrCreate()
    )
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

    aqi_readings = (
        raw.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), aqi_schema).alias("a"))
        .select("a.*")
        .withColumn("event_time", col("timestamp").cast("timestamp"))
        .filter(col("aqi").isNotNull())
        .withWatermark("event_time", "5 minutes")
    )

    aqi_readings.createOrReplaceTempView("aqi_readings")

    # (a) 10-minute rolling average AQI per zone, sliding every 1 minute.
    rolling_avg = spark.sql("""
        SELECT
            window.start AS window_start,
            window.end   AS window_end,
            zone,
            AVG(aqi) AS rolling_avg_aqi
        FROM aqi_readings
        GROUP BY window(event_time, '10 minutes', '1 minute'), zone
    """)

    # (b) Join with the static zone_profile reference table.
    zone_profile = spark.read.option("header", True).option("inferSchema", True).csv(ZONE_PROFILE_CSV)
    enriched = rolling_avg.join(broadcast(zone_profile), on="zone", how="inner")

    # (c) Filter for Unhealthy AQI and write to health_advisories (Update mode).
    advisories = (
        enriched
        .filter(col("rolling_avg_aqi") > UNHEALTHY_THRESHOLD)
        .select(
            "zone", "zone_name", "population", "num_schools",
            "window_start", "window_end", "rolling_avg_aqi",
        )
    )

    query = (
        advisories
        .select(col("zone").alias("key"), to_json(struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", SINK_TOPIC)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="1 minute")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
