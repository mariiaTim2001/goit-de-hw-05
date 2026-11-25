"""Крок 2. Імітація одного датчика.

Один запуск = один датчик із випадковим, але постійним для цього запуску ID.
Щоб імітувати кілька датчиків, запустіть скрипт у кількох терміналах.

    python sensor_producer.py               # нескінченно, кожні 2 с
    python sensor_producer.py -i 1 -n 20    # 20 повідомлень з інтервалом 1 с
"""

import argparse
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from configs import TOPIC_SENSORS, json_serializer, kafka_config, key_serializer


def parse_args():
    parser = argparse.ArgumentParser(description="IoT sensor simulator")
    parser.add_argument("-i", "--interval", type=float, default=2.0, help="seconds between readings")
    parser.add_argument("-n", "--count", type=int, default=0, help="number of readings (0 = infinite)")
    return parser.parse_args()


def make_reading(sensor_id: int) -> dict:
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "temperature": round(random.uniform(25, 45), 2),
        "humidity": round(random.uniform(15, 85), 2),
    }


def main():
    args = parse_args()
    sensor_id = random.randint(1000, 9999)

    producer = KafkaProducer(
        **kafka_config(),
        key_serializer=key_serializer,
        value_serializer=json_serializer,
    )
    print(f"Sensor {sensor_id} started -> topic '{TOPIC_SENSORS}'")

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            reading = make_reading(sensor_id)
            producer.send(TOPIC_SENSORS, key=sensor_id, value=reading)
            producer.flush()
            sent += 1
            print(
                f"[sensor {sensor_id}] #{sent} sent: "
                f"t={reading['temperature']}°C, h={reading['humidity']}% at {reading['timestamp']}"
            )
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.close()
        print(f"Sensor {sensor_id}: {sent} readings sent.")


if __name__ == "__main__":
    main()
