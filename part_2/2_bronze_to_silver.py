from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re
from configs import tables

# Абсолютний шлях для Docker
BASE_DIR = "/opt/airflow/dags"


# Функція для очищення тексту
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text)) if text else None

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    clean_text_udf = udf(clean_text, StringType())

    for table in tables:
        print(f"=== Обробка таблиці {table} ===")
        # Читаємо з абсолютного шляху
        df = spark.read.parquet(f"{BASE_DIR}/bronze/{table}")

        # Очищення всіх текстових колонок
        for col_name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(col_name, clean_text_udf(col(col_name)))

        # Дедублікація
        df = df.dropDuplicates()

        # Збереження в абсолютний шлях
        output_path = f"{BASE_DIR}/silver/{table}"
        df.write.mode("overwrite").parquet(output_path)
        
        print(f"=== {table}: Збережено у {output_path} ===")
        df.show(5)