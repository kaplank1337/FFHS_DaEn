#!/usr/bin/env python3
"""
Teil 9 Pipeline: Kafka -> Spark Structured Streaming -> Cassandra Result Cache -> Visualisierung

Ausführen auf bd-1 zum Beispiel:

spark-submit \
  --master spark://bd-1:7077 \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=2 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.cassandra.connection.host=bd-2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  teil_9_pipeline.py

Optional:
  python Parameter bei spark-submit am Ende anhängen, z. B.
  teil_9_pipeline.py --run-seconds 90 --starting-offsets earliest --plot-date 2026-05-31
"""

import argparse
import sys
import time

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def parse_args():
    parser = argparse.ArgumentParser(description="Teil 9: Kafka -> Spark -> Cassandra -> Visualisierung")
    parser.add_argument("--kafka-bootstrap-servers", default="bd-1:9092")
    parser.add_argument("--topic", default="sensor.barometer")
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--cassandra-host", default="bd-2")
    parser.add_argument("--keyspace", default="sensor_cache")
    parser.add_argument("--table", default="barometer_window_results")
    parser.add_argument(
        "--checkpoint-location",
        default="hdfs://bd-1:9000/data/checkpoint/cassandra_barometer_result_cache",
    )
    parser.add_argument("--run-seconds", type=int, default=60)
    parser.add_argument("--walk-id", default="walk-2026-03-04-01")
    parser.add_argument("--sensor-type", default="barometer")
    parser.add_argument("--filter-year", type=int, default=2026)
    parser.add_argument("--plot-date", default="2026-05-31")
    parser.add_argument("--plot-output", default="barometer_cassandra_result_cache.png")
    return parser.parse_args()


def create_spark_session(args):
    spark = (
        SparkSession.builder
        .appName("Teil_9_Kafka_Spark_Cassandra_Result_Cache")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.cassandra.connection.host", args.cassandra_host)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def print_section(title):
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)


def stop_active_streams(spark):
    active_queries = spark.streams.active
    if not active_queries:
        print("Keine aktiven Streaming Queries gefunden.")
        return

    for query in active_queries:
        print(f"Stoppe aktive Query: name={query.name}, id={query.id}")
        query.stop()

    print("Alle aktiven Streaming Queries wurden gestoppt.")


def write_to_cassandra(batch_df, batch_id, keyspace, table):
    print(f"Batch {batch_id}: schreibe nach Cassandra")

    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: leerer Batch, kein Schreibvorgang notwendig")
        return

    (
        batch_df
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace=keyspace,
            table=table,
        )
        .option("spark.cassandra.output.consistency.level", "ONE")
        .save()
    )

    print(f"Batch {batch_id}: erfolgreich nach Cassandra geschrieben")


def read_cassandra_results(spark, args):
    cached_results_df = (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(
            keyspace=args.keyspace,
            table=args.table,
        )
        .load()
    )

    filtered_cache_df = (
        cached_results_df
        .filter(F.col("walk_id") == args.walk_id)
        .filter(F.col("sensor_type") == args.sensor_type)
        .filter(F.year(F.col("window_start")) == args.filter_year)
        .select(
            "walk_id",
            "sensor_type",
            "window_start",
            "window_end",
            "measurement_count",
            "avg_pressure_hpa",
            "min_pressure_hpa",
            "max_pressure_hpa",
            "stddev_pressure_hpa",
        )
        .orderBy("window_start")
    )

    return filtered_cache_df


def create_plot(filtered_cache_df, args):
    filtered_cache_df_day = (
        filtered_cache_df
        .filter(F.to_date(F.col("window_start")) == F.lit(args.plot_date))
        .orderBy("window_start")
    )

    print_section(f"Zelle 12: Cassandra Resultate für den Tag {args.plot_date}")
    filtered_cache_df_day.show(20, truncate=False)

    pdf_cache = (
        filtered_cache_df_day
        .select(
            "window_start",
            "avg_pressure_hpa",
            "min_pressure_hpa",
            "max_pressure_hpa",
            "measurement_count",
        )
        .orderBy("window_start")
        .toPandas()
    )

    if pdf_cache.empty:
        print(f"Keine passenden Daten für den Tag {args.plot_date} gefunden. Es wird kein Plot erstellt.")
        return

    pdf_cache = pdf_cache.dropna(subset=["window_start", "avg_pressure_hpa"])
    pdf_cache = pdf_cache.sort_values("window_start")

    if pdf_cache.empty:
        print("Nach dem Entfernen von Null-Werten sind keine Daten mehr vorhanden. Es wird kein Plot erstellt.")
        return

    plt.figure(figsize=(14, 6))

    plt.plot(
        pdf_cache["window_start"],
        pdf_cache["avg_pressure_hpa"],
        marker="o",
        linewidth=2,
        markersize=4,
        label="Durchschnitt hPa",
    )

    plt.fill_between(
        pdf_cache["window_start"],
        pdf_cache["min_pressure_hpa"],
        pdf_cache["max_pressure_hpa"],
        alpha=0.2,
        label="Min-Max Bereich",
    )

    plt.xlabel("Zeitfenster")
    plt.ylabel("Luftdruck in hPa")
    plt.title("Barometerdaten der Heimfahrt aus Cassandra Result Cache")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(args.plot_output, dpi=150)
    plt.close()

    print(f"Visualisierung gespeichert unter: {args.plot_output}")


def main():
    args = parse_args()

    print_section("Zelle 1: Spark Session erstellen und System Check")
    spark = create_spark_session(args)

    print("Spark Version:", spark.version)
    print("Spark Master:", spark.sparkContext.master)
    print("Cassandra Host:", spark.conf.get("spark.cassandra.connection.host", "nicht gesetzt"))
    spark.sql("SELECT 1 AS ok").show()

    print_section("Zelle 2: Alte Streaming Queries stoppen")
    stop_active_streams(spark)

    print_section("Zelle 3: Kafka Stream lesen")
    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )
    raw_kafka_df.printSchema()

    print_section("Zelle 4: JSON parsen")
    message_schema = StructType([
        StructField("userId", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("walkId", StringType(), True),
        StructField("sensorType", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("data", StructType([
            StructField("timeSeconds", DoubleType(), True),
            StructField("pressureHpa", DoubleType(), True),
        ]), True),
    ])

    barometer_df = (
        raw_kafka_df
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafkaTimestamp"),
            F.col("key").cast("string").alias("messageKey"),
            F.col("value").cast("string").alias("jsonValue"),
        )
        .withColumn("json", F.from_json(F.col("jsonValue"), message_schema))
        .select(
            "topic",
            "partition",
            "offset",
            "kafkaTimestamp",
            "messageKey",
            "jsonValue",
            F.col("json.userId").alias("userId"),
            F.col("json.deviceId").alias("deviceId"),
            F.col("json.walkId").alias("walkId"),
            F.col("json.sensorType").alias("sensorType"),
            F.to_timestamp(F.from_unixtime(F.col("json.timestamp"))).alias("eventTime"),
            F.col("json.data.timeSeconds").alias("timeSeconds"),
            F.col("json.data.pressureHpa").alias("pressureHpa"),
        )
    )
    barometer_df.printSchema()

    print_section("Zelle 5: Window + Watermark Analyse")
    windowed_barometer_df = (
        barometer_df
        .withWatermark("eventTime", "1 minute")
        .groupBy(
            F.window(F.col("eventTime"), "10 seconds", "5 seconds"),
            F.col("walkId"),
            F.col("deviceId"),
            F.col("sensorType"),
        )
        .agg(
            F.count("*").alias("measurementCount"),
            F.avg("pressureHpa").alias("avgPressureHpa"),
            F.min("pressureHpa").alias("minPressureHpa"),
            F.max("pressureHpa").alias("maxPressureHpa"),
            F.stddev("pressureHpa").alias("stddevPressureHpa"),
        )
        .select(
            F.col("window.start").alias("windowStart"),
            F.col("window.end").alias("windowEnd"),
            "walkId",
            "deviceId",
            "sensorType",
            "measurementCount",
            "avgPressureHpa",
            "minPressureHpa",
            "maxPressureHpa",
            "stddevPressureHpa",
        )
    )
    windowed_barometer_df.printSchema()

    print_section("Zelle 6: Cassandra Result DataFrame vorbereiten")
    cassandra_result_df = (
        windowed_barometer_df
        .select(
            F.col("walkId").alias("walk_id"),
            F.col("deviceId").alias("device_id"),
            F.col("sensorType").alias("sensor_type"),
            F.col("windowStart").alias("window_start"),
            F.col("windowEnd").alias("window_end"),
            F.col("measurementCount").cast("long").alias("measurement_count"),
            F.col("avgPressureHpa").alias("avg_pressure_hpa"),
            F.col("minPressureHpa").alias("min_pressure_hpa"),
            F.col("maxPressureHpa").alias("max_pressure_hpa"),
            F.col("stddevPressureHpa").alias("stddev_pressure_hpa"),
            F.current_timestamp().alias("updated_at"),
        )
    )
    cassandra_result_df.printSchema()

    print_section("Zelle 7: Cassandra Schreibfunktion definieren")
    print("Die Funktion write_to_cassandra wird pro Micro-Batch durch foreachBatch aufgerufen.")

    def write_batch(batch_df, batch_id):
        write_to_cassandra(batch_df, batch_id, args.keyspace, args.table)

    print_section("Zelle 8: Cassandra Streaming Write starten")
    cassandra_query = (
        cassandra_result_df
        .writeStream
        .foreachBatch(write_batch)
        .outputMode("update")
        .option("checkpointLocation", args.checkpoint_location)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print("Cassandra Streaming Query gestartet.")

    print_section("Zelle 9: Streaming Query laufen lassen und Fortschritt ausgeben")
    end_time = time.time() + args.run_seconds

    while time.time() < end_time:
        print("Aktiv:", cassandra_query.isActive)
        print("Status:", cassandra_query.status)
        print("Exception:", cassandra_query.exception())
        print("Last Progress:", cassandra_query.lastProgress)
        print("-" * 100)
        time.sleep(10)

        if not cassandra_query.isActive:
            print("Cassandra Streaming Query ist nicht mehr aktiv.")
            break

    print_section("Zelle 10: Cassandra Ergebnisse lesen")
    filtered_cache_df = read_cassandra_results(spark, args)
    filtered_cache_df.show(20, truncate=False)

    print_section("Zelle 11: Diagnose Zeitraum und Zeilenanzahl")
    filtered_cache_df.select(
        F.min("window_start").alias("min_window_start"),
        F.max("window_start").alias("max_window_start"),
        F.count("*").alias("row_count"),
    ).show(truncate=False)

    filtered_cache_df.groupBy(
        F.to_date("window_start").alias("date")
    ).count().orderBy("date").show(truncate=False)

    print_section("Zelle 12/13: Visualisierung aus Cassandra Result Cache erstellen")
    create_plot(filtered_cache_df, args)

    print_section("Zelle 14: Streaming Query sauber stoppen")
    if cassandra_query.isActive:
        cassandra_query.stop()
        print("Cassandra Streaming Query gestoppt.")

    spark.stop()
    print("Spark Session beendet.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Abbruch durch Benutzer.")
        sys.exit(130)
    except Exception as exc:
        print("Fehler in Teil 9 Pipeline:", repr(exc))
        raise
