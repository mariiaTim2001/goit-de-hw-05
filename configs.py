"""Спільна конфігурація Kafka та імена топіків.

За замовчуванням підключення до локального брокера (docker-compose.yml).
Для віддаленого брокера задайте змінні середовища:
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM, KAFKA_USERNAME, KAFKA_PASSWORD
"""

import json
import os

# Ідентифікатор, який додається до імен топіків, щоб вони не дублювалися
MY_NAME = os.getenv("MY_NAME", "mariia_timofieieva")

TOPIC_SENSORS = f"{MY_NAME}_building_sensors"
TOPIC_TEMPERATURE_ALERTS = f"{MY_NAME}_temperature_alerts"
TOPIC_HUMIDITY_ALERTS = f"{MY_NAME}_humidity_alerts"

# Порогові значення
TEMPERATURE_MAX = 40
HUMIDITY_MIN = 20
HUMIDITY_MAX = 80


def kafka_config() -> dict:
    """Параметри підключення, спільні для producer, consumer та admin client."""
    config = {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
        "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    }
    if config["security_protocol"].startswith("SASL"):
        config.update(
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_plain_username=os.environ["KAFKA_USERNAME"],
            sasl_plain_password=os.environ["KAFKA_PASSWORD"],
        )
    return config


def json_serializer(value) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def json_deserializer(raw: bytes):
    return json.loads(raw.decode("utf-8"))


def key_serializer(key) -> bytes:
    return str(key).encode("utf-8")
