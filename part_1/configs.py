# configs.py

# Налаштування Kafka
kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}

my_name = "alexdtsc"

# Топік, КУДИ ми відправляємо сирі результати змагань
topic_input = f'{my_name}_athlete_event_results'

# Топік, КУДИ ми відправляємо готові агреговані дані
topic_output = f'{my_name}_athlete_enriched_agg'

# Налаштування MySQL
db_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver"
}