# 1_producer.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from configs import kafka_config, db_config, topic_input

# Підключаємо пакети Kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 pyspark-shell'

spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("OlympicProducer") \
    .getOrCreate()


# Етап 3
'''
3-a. Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results. 
Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results. 
Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.
'''

# Читаємо результати змагань з бази даних
print("Читаємо дані з MySQL (athlete_event_results)...")

df_results = spark.read.format("jdbc").options(
    url=db_config["url"],
    driver=db_config["driver"],
    dbtable="(SELECT * FROM athlete_event_results LIMIT 500) AS subset_data", # Сира таблиця  -> як варіант для зменшення часу очікування для експерименту можемо взяти невелику вибірку з таблиці: dbtable=athlete_event_results
    user=db_config["user"],
    password=db_config["password"]
).load()


# Перетворюємо всі колонки на один великий JSON і називаємо цю колонку "value" (цього вимагає Kafka)
df_kafka_ready = df_results.select(to_json(struct("*")).alias("value"))


# Налаштування авторизації для Kafka
jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';"


# Відправляємо дані як пакет (batch)
print(f"Відправляємо дані в Kafka топік: {topic_input}...")

df_kafka_ready.write.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .option("topic", topic_input) \
    .save()

print("Успішно відправлено!")