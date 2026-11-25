"""Крок 1. Створення трьох топіків у Kafka."""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import (
    MY_NAME,
    TOPIC_HUMIDITY_ALERTS,
    TOPIC_SENSORS,
    TOPIC_TEMPERATURE_ALERTS,
    kafka_config,
)

NUM_PARTITIONS = 2
REPLICATION_FACTOR = 1

admin_client = KafkaAdminClient(**kafka_config())

for name in (TOPIC_SENSORS, TOPIC_TEMPERATURE_ALERTS, TOPIC_HUMIDITY_ALERTS):
    topic = NewTopic(
        name=name,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
    )
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{name}' already exists.")

print(f"\nTopics containing '{MY_NAME}':")
[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]

admin_client.close()
