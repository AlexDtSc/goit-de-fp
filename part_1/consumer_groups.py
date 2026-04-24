# consumer_groups.py

from kafka import KafkaConsumer
from configs import kafka_config, topic_output
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v is not None else None,
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v is not None else None,
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

# Підписка на тему (topic)
consumer.subscribe([topic_output])

print(f"Subscribed to topic '{topic_output}'")

# Виведення консолі
'''
Subscribed to topic 'alexdtsc_athlete_enriched_agg''
'''

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer

