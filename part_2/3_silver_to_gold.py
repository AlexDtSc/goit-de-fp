# 3_silver_to_gold.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col
from configs import tables
from pyspark.sql.functions import regexp_replace

# Абсолютний шлях для Docker
BASE_DIR = "/opt/airflow/dags"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    print("=== Читання таблиць із Silver ===")

    # Створюємо SparkSession
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Читаємо silver-таблиці з абсолютних шляхів
    athlete_bio = spark.read.parquet(f"{BASE_DIR}/silver/athlete_bio")
    athlete_event_results = spark.read.parquet(f"{BASE_DIR}/silver/athlete_event_results")

    # Перейменовуємо country_noc, щоб уникнути дубля
    athlete_bio = athlete_bio.withColumnRenamed("country_noc", "bio_country_noc")

    # Join по athlete_id
    joined = athlete_event_results.join(
        athlete_bio,
        on="athlete_id",
        how="inner"
    )

    # ПРИВЕДЕННЯ ДО ЧИСЛОВОГО ТИПУ (Замінюємо кому на крапку перед кастом)
    joined = joined.withColumn("height", regexp_replace(col("height"), ",", ".").cast("double")) \
                   .withColumn("weight", regexp_replace(col("weight"), ",", ".").cast("double"))

    # Агрегація: середня вага та зріст для sport, medal, sex, country
    gold = joined.groupBy("sport", "medal", "sex", "country", "bio_country_noc").agg(
        avg("weight").alias("avg_weight"),
        avg("height").alias("avg_height")
    ).withColumn("timestamp", current_timestamp())

    # Запис у gold-layer (абсолютний шлях)
    output_path = f"{BASE_DIR}/gold/avg_stats"
    gold.write.mode("overwrite").parquet(output_path)

    # Виводимо результат у логи для Airflow
    gold.show(10, truncate=False)

    print("Silver to Gold job виконано успішно!")