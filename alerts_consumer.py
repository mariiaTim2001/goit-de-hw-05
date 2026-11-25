"""Крок 5. Читання сповіщень з temperature_alerts та humidity_alerts і виведення на екран."""

from kafka import KafkaConsumer

from configs import (
    MY_NAME,
    TOPIC_HUMIDITY_ALERTS,
    TOPIC_TEMPERATURE_ALERTS,
    json_deserializer,
    kafka_config,
)

LABELS = {
    TOPIC_TEMPERATURE_ALERTS: "TEMPERATURE",
    TOPIC_HUMIDITY_ALERTS: "HUMIDITY",
}


def main():
    consumer = KafkaConsumer(
        TOPIC_TEMPERATURE_ALERTS,
        TOPIC_HUMIDITY_ALERTS,
        **kafka_config(),
        value_deserializer=json_deserializer,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"{MY_NAME}_alerts_consumer",
    )
    print(f"Subscribed to '{TOPIC_TEMPERATURE_ALERTS}' and '{TOPIC_HUMIDITY_ALERTS}'.\n")

    try:
        for record in consumer:
            alert = record.value
            print(
                f"[{LABELS[record.topic]:<11}] sensor {alert['sensor_id']} | "
                f"t={alert['temperature']}°C, h={alert['humidity']}% | "
                f"measured {alert['timestamp']} | {alert['message']}"
            )
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
