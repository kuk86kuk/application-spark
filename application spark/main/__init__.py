from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# 1. Базовые настройки DAG
default_args = {
    'owner': 'data_team',       # Владелец процесса
    'depends_on_past': False,   # Не зависит от предыдущих запусков
    'start_date': datetime(2023, 1, 1),  # Дата начала работы
    'retries': 1                # Количество попыток перезапуска при ошибке
}

# 2. Создаём сам DAG
with DAG(
    'datamart_loader',          # Уникальное имя DAG
    default_args=default_args,   # Применяем настройки
    schedule_interval='@daily',  # Запускать каждый день
    catchup=False,              # Не запускать пропущенные за прошлое время
    tags=['spark', 'hive']      # Теги для поиска
) as dag:
    
    # 3. Операторы (шаги) DAG
    
    # Стартовая точка (просто маркер)
    start = DummyOperator(task_id='start')
    
    # Общие параметры для всех Spark-задач
    common_spark_args = {
        'application': '/opt/spark_apps/main/app.py',  # Путь к вашему Spark-приложению
        'conn_id': 'spark_cluster',  # Подключение к Spark в Airflow
        'application_args': [        # Аргументы для Spark-приложения
            '--sql-base-path=/sql/datamarts/',
            '--env=prod'
        ]
    }
    
    # Задача 1: старт плиложение
    load_transactions = SparkSubmitOperator(
        task_id='load_transaction_mart',  # Уникальный ID задачи
        name='load_transaction_mart',     # Имя для Spark UI
        **common_spark_args,             # Общие параметры
        application_args=common_spark_args['application_args'] + [
            '--mart=transaction'         # Доп. параметр: загружаем транзакции
        ]
    )
    
    # Задача 2: предзагрузка ичтосников  данных
    load_customers = SparkSubmitOperator(
        task_id='load_customer_mart',
        name='load_customer_mart',
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
            '--mart=customer'           # Доп. параметр: загружаем клиентов
        ]
    )
    
    # Задача 3: стг (калк стг)
    load_customers = SparkSubmitOperator(
        task_id='load_customer_mart',
        name='load_customer_mart',
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
            '--mart=customer'           # Доп. параметр: загружаем клиентов
        ]
    )

     # Задача 4: инкримент (калк инк)
    load_customers = SparkSubmitOperator(
        task_id='load_customer_mart',
        name='load_customer_mart',
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
            '--mart=customer'           # Доп. параметр: загружаем клиентов
        ]
    )

     # Задача 5: инкримент (калк инк)
    load_customers = SparkSubmitOperator(
        task_id='load_customer_mart',
        name='load_customer_mart',
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
            '--mart=customer'           # Доп. параметр: загружаем клиентов
        ]
    )

    # Конечная точка (просто маркер)
    end = DummyOperator(task_id='end')
    
    # 4. Определяем порядок выполнения
    start >> [load_transactions, load_customers] >> end