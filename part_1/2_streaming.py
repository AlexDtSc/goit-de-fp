# 2_streaming.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from configs import kafka_config, db_config, topic_input, topic_output

# Підключаємо пакети Kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 pyspark-shell'

spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("OlympicStreamingApp") \
    .getOrCreate()

jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';"

# ==============================================================================
# ЕТАП 1-2: Зчитування та фільтрація athlete_bio (Наша статична "Енциклопедія")
# ==============================================================================
df_bio = spark.read.format('jdbc').options(
    url=db_config["url"], driver=db_config["driver"], dbtable="athlete_bio",
    user=db_config["user"], password=db_config["password"]
).load()

# ВИПРАВЛЕНИЙ ФІЛЬТР: Використовуємо SQL-команду try_cast для безпечної перевірки
df_bio_filtered = df_bio.filter(
    "try_cast(height as double) IS NOT NULL AND try_cast(weight as double) IS NOT NULL"
)


# Spark підказує нам використати функцію try_cast. Вона працює так: "Спробуй зробити з цього число. Якщо вийде — супер. Якщо там лежить сміття — просто тихенько поверни NULL без криків і помилок".
# Чому це круто: Ми передали Spark-у звичайний SQL-рядок. Він сам безпечно спробує перетворити зріст і вагу на double. Ті рядки, де були порожні значення '' або текст, стануть NULL і миттєво відфільтруються завдяки IS NOT NULL.



# ==============================================================================
# ЕТАП 3: Зчитування з Kafka (Наш "Прямий ефір") та парсинг JSON
# ==============================================================================
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .option("subscribe", topic_input) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()

# Структура JSON, який летить із Kafka
json_schema = StructType([
    StructField("athlete_id", IntegerType()),
    StructField("sport", StringType()),
    StructField("medal", StringType()),
    StructField("country_noc", StringType())
])

# Kafka віддає дані у бінарному вигляді (binary). Ми робимо з них текст (STRING), 
# а потім розпаковуємо (from_json) у нормальні колонки.
parsed_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), json_schema).alias("data")) \
    .select("data.*")

# ==============================================================================
# ЕТАП 4-5: Об'єднання (JOIN) та Агрегація
# ==============================================================================
# 1. Видаляємо дублюючу колонку з біометрії, щоб Spark не заплутався
df_bio_clean = df_bio_filtered.drop("country_noc")

# 2. З'єднуємо потік із бази результатів зі статичною базою біометрії
joined_stream = parsed_stream.join(df_bio_clean, "athlete_id", "inner")

# 3. Знаходимо середній зріст і вагу, групуючи за 4 параметрами
agg_stream = joined_stream.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    )

# 4. Додаємо мітку часу (коли саме відбувся розрахунок)
final_stream = agg_stream.withColumn("timestamp", current_timestamp())

# ==============================================================================
# ЕТАП 6: Відправка результатів (forEachBatch)
# ==============================================================================
# Ця функція викликається для кожної нової "порції" (мікробатчу) оброблених даних
def process_batch(batch_df, batch_id):

    # 6.a Відправка в базу даних (MySQL)
    batch_df.write.format("jdbc").options(
        url=db_config["url"], driver=db_config["driver"],
        dbtable="alexdtsc_athlete_enriched_agg", # Наша нова таблиця
        user=db_config["user"], password=db_config["password"]
    ).mode("append").save()

    # 6.b Відправка у вихідний Kafka топік
    # Для Kafka треба знову запакувати всі колонки в один JSON (колонку "value")
    kafka_output_df = batch_df.select(to_json(struct("*")).alias("value"))
    
    kafka_output_df.write.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
        .option("kafka.security.protocol", kafka_config["security_protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config", jaas_config) \
        .option("topic", topic_output) \
        .save()

# Запускаємо конвеєр і чекаємо
query = final_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

query.awaitTermination()