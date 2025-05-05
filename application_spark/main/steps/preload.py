from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    "DataMart_transaction1_flow",
    schedule_interval="@daily",
    start_date=false,
    catchup=False,
    tags=["jenkins", "DataMart_transaction1"]
) as dag:

    start = SparkSubmitOperator(
        task_id='start',  
        name='start',     
        **common_spark_args
    )
    
    
    stage_preload = PythonOperator(
        task_id="stage_preload",
        name=stage_preload,
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
        [source:{{ params.source1 }}, backup_source:{{ params.source2 }}, processing_date:{{ params.date_9999 }}]
        ]
        )

    stage_calc_stg = PythonOperator(
        task_id="stage_calc_stg",
        name=stage_calc_stg,
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
        [main_source:{{ params.source1 }}, reference_date:{{ params.date_9999 }}]
        ]
        )

    stage_check_stg = PythonOperator(
        task_id="stage_check_stg",
        name=stage_check_stg,
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
        [source_to_validate:{{ params.source1 }}, secondary_source:{{ params.source2 }}, check_date:{{ params.date_9999 }}]
        ]
        )

    stage_calc_inc = PythonOperator(
        task_id="stage_calc_inc",
        name=stage_calc_inc,
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
        [primary_source:{{ params.source1 }}, delta_date:{{ params.date_9999 }}]
        ]
        )

    stage_check_inc = PythonOperator(
        task_id="stage_check_inc",
        name=stage_check_inc,
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
        [validation_sources:[{{ params.source1 }}, {{ params.source2 }}], snapshot_date:{{ params.date_9999 }}]
        ]
        )

    stage_MTP = PythonOperator(
        task_id="stage_MTP",
        name=stage_MTP,
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
        [data_sources:[main:{{ params.source1 }}, supplementary:{{ params.source2 }}], load_date:{{ params.date_9999 }}]
        ]
        )

    stage_hist = PythonOperator(
        task_id="stage_hist",
        name=stage_hist,
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
        [history_sources:[{{ params.source1 }}, {{ params.source2 }}], effective_date:{{ params.date_9999 }}]
        ]
        )

    final_check = PythonOperator(
        task_id="final_check",
        name=final_check,
        **common_spark_args,
        application_args=common_spark_args['application_args'] + [
        [all_sources:[primary:{{ params.source1 }}, secondary:{{ params.source2 }}], report_date:{{ params.date_9999 }}]
        ]
        )


    final = SparkSubmitOperator(
        task_id='final',  
        name='final',     
        **common_spark_args
    )

     start >> stage_preload
stage_preload >> stage_calc_stg
    stage_calc_stg >> stage_check_stg
    stage_check_stg >> stage_calc_inc
    stage_calc_inc >> stage_check_inc
    stage_check_inc >> stage_MTP
    stage_MTP >> stage_hist
    stage_hist >> final_check >> final
