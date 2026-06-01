from kafka import KafkaProducer
import csv
import json
import time

CSV_FILE = "/home/tirsus/Parallels_sharing_bd1/sensordaten/Barometer.csv"
TOPIC = "sensor.barometer"
BOOTSTRAP_SERVER = "bd-1:9092"

USER_ID = "user1"
DEVICE_ID = "phone1"
WALK_ID = "walk-2026-03-04-01"
SENSOR_TYPE = "barometer"


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8")
    )

    with open(CSV_FILE, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            time_seconds = float(row["Time (s)"])
            pressure_hpa = float(row["X (hPa)"])

            message = {
                "userId": USER_ID,
                "deviceId": DEVICE_ID,
                "walkId": WALK_ID,
                "sensorType": SENSOR_TYPE,
                "timestamp": time.time(),
                "data": {
                    "timeSeconds": time_seconds,
                    "pressureHpa": pressure_hpa
                }
            }

            future = producer.send(
                TOPIC,
                key=WALK_ID,
                value=message
            )

            metadata = future.get(timeout=60)

            print(
                f"Gesendet -> topic={metadata.topic}, "
                f"partition={metadata.partition}, "
                f"offset={metadata.offset}, "
                f"message={message}"
            )

            time.sleep(0.5)

    producer.flush()
    producer.close()
    print("Fertig: Alle CSV-Zeilen wurden an Kafka gesendet.")


if __name__ == "__main__":
    main()
