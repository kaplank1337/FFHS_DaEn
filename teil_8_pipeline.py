#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teil 8 - Kafka + Spark Structured Streaming + HDFS Bronze + Live-Visualisierung

Dieses Skript entspricht den Notebook-Zellen in sequentieller Reihenfolge.

Ausführen auf bd-1 am besten mit:

spark-submit \
  --master spark://bd-1:7077 \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=2 \
  --conf spark.dynamicAllocation.enabled=false \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  teil_8_pipeline.py

Wichtig:
- Kafka muss laufen.
- Topic sensor.barometer muss existieren.
- Producer kann bei startingOffsets="earliest" vorher oder währenddessen laufen.
- Für eine echte Live-Demo mit nur neuen Daten kannst du STARTING_OFFSETS auf "latest" ändern.
"""

import time
import traceback

import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# =============================================================================
# Konfiguration
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = "bd-1:9092"
KAFKA_TOPIC = "sensor.barometer"

# Für stabile Demo: "earliest".
# Für echte Live-Demo: "latest", dann Producer erst nach Query-Start starten.
STARTING_OFFSETS = "earliest"

HDFS_BRONZE_PATH = "hdfs://bd-1:9000/data/bronze/sensor_barometer"
HDFS_BRONZE_CHECKPOINT = "hdfs://bd-1:9000/data/checkpoint/bronze_sensor_barometer"

MEMORY_QUERY_NAME = "barometer_realtime_analysis"

WINDOW_DURATION = "10 seconds"
WINDOW_SLIDE = "5 seconds"
WATERMARK_DELAY = "1 minute"

VISUALIZATION_ITERATIONS = 60
VISUALIZATION_SLEEP_SECONDS = 5
PLOT_LAST_N_WINDOWS = 100


# =============================================================================
# Zelle 1: Spark Session erstellen und prüfen
# =============================================================================

spark = (
    SparkSession.builder
    .appName("Teil_8_Kafka_Spark_HDFS_Bronze")
    .master("spark://bd-1:7077")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.executor.cores", "1")
    .config("spark.cores.max", "2")
    .config("spark.dynamicAllocation.enabled", "false")
    # Falls das Skript direkt mit python3 statt spark-submit gestartet wird,
    # versucht Spark darüber den Kafka-Connector zu laden.
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark Version:", spark.version)
print("Spark Master:", spark.sparkContext.master)
spark.sql("SELECT 1 AS ok").show()


# =============================================================================
# Zelle 2: Alte Streaming Queries stoppen
# =============================================================================

for query in spark.streams.active:
    print("Stoppe bestehende Query:", query.name)
    query.stop()

print("Alle aktiven Streaming Queries gestoppt.")


# =============================================================================
# Zelle 3: Kafka Stream lesen
# =============================================================================

raw_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .load()
)

print("Schema raw_kafka_df:")
raw_kafka_df.printSchema()


# =============================================================================
# Zelle 4: JSON parsen
# =============================================================================

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
        F.col("value").cast("string").alias("jsonValue")
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
        F.col("json.data.pressureHpa").alias("pressureHpa")
    )
)

print("Schema barometer_df:")
barometer_df.printSchema()


# =============================================================================
# Zelle 5: Window + Watermark Analyse
# =============================================================================

windowed_barometer_df = (
    barometer_df
    .withWatermark("eventTime", WATERMARK_DELAY)
    .groupBy(
        F.window(F.col("eventTime"), WINDOW_DURATION, WINDOW_SLIDE),
        F.col("walkId"),
        F.col("deviceId"),
        F.col("sensorType")
    )
    .agg(
        F.count("*").alias("measurementCount"),
        F.avg("pressureHpa").alias("avgPressureHpa"),
        F.min("pressureHpa").alias("minPressureHpa"),
        F.max("pressureHpa").alias("maxPressureHpa"),
        F.stddev("pressureHpa").alias("stddevPressureHpa")
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
        "stddevPressureHpa"
    )
)

print("Schema windowed_barometer_df:")
windowed_barometer_df.printSchema()


# =============================================================================
# Zelle 6: Memory Query für Echtzeit-Visualisierung starten
# =============================================================================

analysis_query = (
    windowed_barometer_df
    .writeStream
    .format("memory")
    .queryName(MEMORY_QUERY_NAME)
    .outputMode("complete")
    .trigger(processingTime="5 seconds")
    .start()
)

print("analysis_query gestartet.")


# =============================================================================
# Zelle 7: Bronze DataFrame für HDFS vorbereiten
# =============================================================================

bronze_df = (
    barometer_df
    .withColumn("ingestTime", F.current_timestamp())
    .withColumn("eventDate", F.to_date(F.col("eventTime")))
    .select(
        "topic",
        "partition",
        "offset",
        "messageKey",
        "jsonValue",
        "userId",
        "deviceId",
        "walkId",
        "sensorType",
        "eventTime",
        "kafkaTimestamp",
        "ingestTime",
        "timeSeconds",
        "pressureHpa",
        "eventDate"
    )
)

print("Schema bronze_df:")
bronze_df.printSchema()


# =============================================================================
# Zelle 8: Nach HDFS als Parquet schreiben
# =============================================================================

bronze_query = (
    bronze_df
    .writeStream
    .format("parquet")
    .option("path", HDFS_BRONZE_PATH)
    .option("checkpointLocation", HDFS_BRONZE_CHECKPOINT)
    .partitionBy("eventDate", "sensorType")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

print("bronze_query gestartet.")


# =============================================================================
# Zelle 9: Analyse-Ergebnisse prüfen
# =============================================================================

print("Warte kurz, bis die ersten Micro-Batches verarbeitet wurden...")
time.sleep(15)

spark.sql(f"""
    SELECT
        windowStart,
        windowEnd,
        walkId,
        measurementCount,
        avgPressureHpa,
        minPressureHpa,
        maxPressureHpa,
        stddevPressureHpa
    FROM {MEMORY_QUERY_NAME}
    ORDER BY windowStart DESC
    LIMIT 20
""").show(truncate=False)

print("analysis_query aktiv:", analysis_query.isActive)
print("analysis_query exception:", analysis_query.exception())
print("analysis_query lastProgress:", analysis_query.lastProgress)

print("bronze_query aktiv:", bronze_query.isActive)
print("bronze_query exception:", bronze_query.exception())
print("bronze_query lastProgress:", bronze_query.lastProgress)


# =============================================================================
# Zelle 10: Live-Visualisierung aus Memory-Tabelle
# =============================================================================

try:
    for i in range(VISUALIZATION_ITERATIONS):
        pdf = (
            spark.sql(f"""
                SELECT
                    windowStart,
                    avgPressureHpa,
                    minPressureHpa,
                    maxPressureHpa,
                    measurementCount
                FROM {MEMORY_QUERY_NAME}
                ORDER BY windowStart
            """)
            .toPandas()
        )

        print(f"Iteration: {i + 1}/{VISUALIZATION_ITERATIONS}")
        print("Anzahl Zeilen:", len(pdf))
        print("analysis_query aktiv:", analysis_query.isActive)
        print("analysis_query exception:", analysis_query.exception())

        if pdf.empty:
            print("Noch keine Daten empfangen.")
        else:
            pdf = pdf.dropna(subset=["windowStart", "avgPressureHpa"])
            pdf = pdf.sort_values("windowStart")
            pdf_plot = pdf.tail(PLOT_LAST_N_WINDOWS)

            plt.figure(figsize=(12, 5))

            plt.plot(
                pdf_plot["windowStart"],
                pdf_plot["avgPressureHpa"],
                marker="o",
                linewidth=2,
                markersize=4,
                label="Durchschnitt hPa"
            )

            plt.fill_between(
                pdf_plot["windowStart"],
                pdf_plot["minPressureHpa"],
                pdf_plot["maxPressureHpa"],
                alpha=0.2,
                label="Min-Max Bereich"
            )

            plt.xlabel("Zeitfenster")
            plt.ylabel("Luftdruck in hPa")
            plt.title("Barometerdaten in Echtzeit aus Spark Memory Table")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()

            output_file = "barometer_live_visualisierung.png"
            plt.savefig(output_file)
            plt.close()

            print(f"Visualisierung gespeichert: {output_file}")

        time.sleep(VISUALIZATION_SLEEP_SECONDS)

except KeyboardInterrupt:
    print("Visualisierung durch Benutzer beendet.")

except Exception:
    print("Fehler während der Visualisierung:")
    traceback.print_exc()

finally:
    # =============================================================================
    # Zelle 11: HDFS-Daten lesen und zeigen
    # =============================================================================
    print("Versuche HDFS Bronze Daten zu lesen...")

    try:
        bronze_read_df = spark.read.parquet(HDFS_BRONZE_PATH)
        bronze_read_df.printSchema()
        bronze_read_df.show(20, truncate=False)
    except Exception:
        print("Konnte Bronze-Daten noch nicht lesen. Eventuell wurden noch keine Parquet-Dateien geschrieben.")
        traceback.print_exc()

    # =============================================================================
    # Zelle 12: Streaming Queries sauber stoppen
    # =============================================================================
    print("Stoppe Streaming Queries...")

    for query in spark.streams.active:
        print("Stoppe Query:", query.name)
        query.stop()

    print("Alle Streaming Queries gestoppt.")
    spark.stop()
    print("Spark Session gestoppt.")
