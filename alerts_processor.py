"""Кроки 3–4. Обробка даних датчиків.

Читає building_sensors, перевіряє показники та надсилає сповіщення:
  - температура > 40°C                 -> temperature_alerts
  - вологість > 80% або < 20%          -> humidity_alerts
"""

from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from configs import (
    HUMIDITY_MAX,
    HUMIDITY_MIN,
    MY_NAME,
    TEMPERATURE_MAX,
    TOPIC_HUMIDITY_ALERTS,
    TOPIC_SENSORS,
    TOPIC_TEMPERATURE_ALERTS,
    json_deserializer,
    json_serializer,
    kafka_config,
    key_serializer,
)


def build_alert(reading: dict, message: str) -> dict:
    return {
        "sensor_id": reading["sensor_id"],
        "timestamp": reading["timestamp"],
        "temperature": reading["temperature"],
        "humidity": reading["humidity"],
        "alert_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "message": message,
    }


def check_reading(reading: dict) -> list[tuple[str, dict]]:
    """Повертає список (топік, сповіщення) для одного показу датчика."""
    alerts = []
    temperature = reading["temperature"]
    humidity = reading["humidity"]

    if temperature > TEMPERATURE_MAX:
        alerts.append((
            TOPIC_TEMPERATURE_ALERTS,
            build_alert(reading, f"Temperature {temperature}°C exceeds threshold {TEMPERATURE_MAX}°C"),
        ))

    if humidity > HUMIDITY_MAX:
        alerts.append((
            TOPIC_HUMIDITY_ALERTS,
            build_alert(reading, f"Humidity {humidity}% exceeds upper threshold {HUMIDITY_MAX}%"),
        ))
    elif humidity < HUMIDITY_MIN:
        alerts.append((
            TOPIC_HUMIDITY_ALERTS,
            build_alert(reading, f"Humidity {humidity}% is below lower threshold {HUMIDITY_MIN}%"),
        ))

    return alerts


def main():
    consumer = KafkaConsumer(
        TOPIC_SENSORS,
        **kafka_config(),
        value_deserializer=json_deserializer,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"{MY_NAME}_alerts_processor",
    )
    producer = KafkaProducer(
        **kafka_config(),
        key_serializer=key_serializer,
        value_serializer=json_serializer,
    )
    print(f"Processor subscribed to '{TOPIC_SENSORS}'. Waiting for data...\n")

    try:
        for record in consumer:
            reading = record.value
            alerts = check_reading(reading)
            status = "ALERT" if alerts else "ok   "
            print(
                f"[{status}] sensor {reading['sensor_id']}: "
                f"t={reading['temperature']}°C, h={reading['humidity']}% at {reading['timestamp']}"
            )
            for topic, alert in alerts:
                producer.send(topic, key=alert["sensor_id"], value=alert)
                print(f"        -> sent to '{topic}': {alert['message']}")
            if alerts:
                producer.flush()
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
