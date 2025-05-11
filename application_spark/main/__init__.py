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
    
 
    
    # Общие параметры для всех Spark-задач
    common_spark_args = {
        'application':'hdfs:///user/jenkins/spark-apps/main/app.py',
        'conn_id': 'spark_cluster',  # Подключение к Spark в Airflow
        'application_args': [        # Аргументы для Spark-приложения
            '--sql-base-path=/sql/datamarts/',
            '--env=prod'
        ]
    }
    
    # Задача 1: старт плиложение
    load_transactions = SparkSubmitOperator(
        task_id='start',  # Уникальный ID задачи
        name='start',     # Имя для Spark UI
        **common_spark_args,             # Общие параметры
        application_args=common_spark_args['application_args'] + [
            '--mart=transaction'         # Доп. параметр: загружаем транзакции
        ]
    )
    

   # Задача 8:  финиш
    load_customers = SparkSubmitOperator(
        task_id='hist_chek',
        name='hist_chek',
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
            '--mart=customer'           # Доп. параметр: загружаем клиентов
        ]
    )
    
    
    # 4. Определяем порядок выполнения
    start >>  end