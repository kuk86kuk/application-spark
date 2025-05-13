from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'spark_full_data_pipeline',
    default_args=default_args,
    description='Complete Spark data processing pipeline with all stages',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['spark', 'prod'],
    max_active_runs=1,
)

def create_spark_task(
    task_id: str,
    env: str,
    step_name: str,
    query_mapping: str = "_",
    datamart: str = "_",
    query_path: str = "_",
    table_schema: str = "_",
    table_name: str = "_",
    repartition: str = "40",
    partition_by: str = "_",
    bucket_by: str = "_",
    num_buckets: str = "300",
    location: str = "_",
    do_truncate_table: str = "n",
    do_drop_table: str = "n",
    do_msck_repair_table: str = "n",
    temp_view_name: str = "_",
    cache_df: str = "n"
):
    """Функция для создания Spark задач с полным набором параметров"""
    return SparkSubmitOperator(
        task_id=f'spark_{task_id}',
        application='hdfs://namenode:8020/user/jenkins/application_spark/app.py',
        conn_id='spark_default',
        verbose=True,
        application_args=[
            '--env', env,
            '--step', step_name,
            '--task-id', task_id,
            '--query_mapping', query_mapping,
            '--datamart', datamart,
            '--query_path', query_path,
            '--table_schema', table_schema,
            '--table_name', table_name,
            '--repartition', repartition,
            '--partition_by', partition_by,
            '--bucket_by', bucket_by,
            '--num_buckets', num_buckets,
            '--location', location,
            '--do_truncate_table', do_truncate_table,
            '--do_drop_table', do_drop_table,
            '--do_msck_repair_table', do_msck_repair_table,
            '--temp_view_name', temp_view_name,
            '--cache_df', cache_df
        ],
        py_files='hdfs://namenode:8020/user/jenkins/application_spark/dependencies.zip',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.hadoop.fs.defaultFS': 'hdfs://namenode:8020',
            'spark.hadoop.dfs.client.use.datanode.hostname': 'true',
            'spark.hadoop.dfs.replication': '1',
            'spark.hadoop.ipc.max.response.size': '104857600',
            'spark.submit.deployMode': 'client',
            'spark.driver.memory': '1g',
            'spark.executor.memory': '1g',
            'spark.executor.instances': '1',
            'spark.executor.cores': '1',
            'spark.network.timeout': '600s',
            'spark.sql.shuffle.partitions': '100'
        },
        dag=dag,
    )

# Начало и конец пайплайна
start_task = DummyOperator(task_id='start_pipeline', dag=dag)
end_task = DummyOperator(task_id='end_pipeline', dag=dag)

# Этап предзагрузки
preload_task = create_spark_task(
    task_id='stage_preload',
    env='prod',
    step_name='preload',
    query_mapping='source:transactions_stg',
    datamart='transactions',
    query_path='hdfs://namenode:8020/queries/preload.sql',
    table_schema='stg',
    table_name='transactions_preload',
    partition_by='load_date',
    do_truncate_table='y'
)

# Этап расчета staging
calc_stg_task = create_spark_task(
    task_id='stage_calc_stg',
    env='prod',
    step_name='calc_stg',
    query_mapping='source:transactions_stg',
    datamart='transactions',
    query_path='hdfs://namenode:8020/queries/calc_stg.sql',
    table_schema='stg',
    table_name='transactions_calculated',
    repartition='20,transaction_date',
    partition_by='transaction_date',
    do_truncate_table='y'
)

# Этап проверки staging
check_stg_task = create_spark_task(
    task_id='stage_check_stg',
    env='prod',
    step_name='check_stg',
    query_mapping='',
    datamart='transactions',
    query_path='hdfs://namenode:8020/queries/check_stg.sql',
    temp_view_name='stg_check_results',
    cache_df='y'
)

# Этап расчета инкремента
calc_inc_task = create_spark_task(
    task_id='stage_calc_inc',
    env='prod',
    step_name='calc_inc',
    query_mapping='source:transactions_inc',
    datamart='transactions',
    query_path='hdfs://namenode:8020/queries/calc_inc.sql',
    table_schema='inc',
    table_name='transactions_incremental',
    repartition='30,processing_date',
    partition_by='processing_date'
)

# Этап проверки инкремента
check_inc_task = create_spark_task(
    task_id='stage_check_inc',
    env='prod',
    step_name='check_inc',
    query_mapping='',
    datamart='transactions',
    query_path='hdfs://namenode:8020/queries/check_inc.sql',
    temp_view_name='inc_check_results',
    cache_df='y'
)

# Этап MTP обработки
mtp_task = create_spark_task(
    task_id='stage_MTP',
    env='prod',
    step_name='MTP',
    query_mapping='',
    datamart='transactions',
    query_path='hdfs://namenode:8020/queries/mtp_processing.sql',
    table_schema='mtp',
    table_name='transactions_processed',
    repartition='40',
    partition_by='process_batch'
)

# Этап исторической обработки
hist_task = create_spark_task(
    task_id='stage_hist',
    env='prod',
    step_name='hist',
    query_mapping='',
    datamart='transactions',
    query_path='hdfs://namenode:8020/queries/historical_load.sql',
    table_schema='hist',
    table_name='transactions_historical',
    repartition='50',
    partition_by='year,month'
)

# Финальная проверка
final_check_task = create_spark_task(
    task_id='final_check',
    env='prod',
    step_name='final_check',
    query_mapping='',
    datamart='transactions',
    query_path='hdfs://namenode:8020/queries/final_validation.sql',
    temp_view_name='final_check_results',
    cache_df='y'
)

# Устанавливаем последовательность выполнения задач
(
    start_task 
    >> preload_task 
    >> calc_stg_task 
    >> check_stg_task 
    >> calc_inc_task 
    >> check_inc_task 
    >> mtp_task 
    >> hist_task 
    >> final_check_task 
    >> end_task
)
