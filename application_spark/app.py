from pyspark.sql import SparkSession
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # Инициализация Spark с увеличенными таймаутами
        spark = SparkSession.builder \
            .appName("DataProcessing") \
            .config("spark.network.timeout", "800s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .getOrCreate()
        
        logger.info("SparkSession успешно инициализирован")
        
        # Ваша основная логика обработки
        process_data(spark)
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            logger.info("Завершение SparkSession")
            spark.stop()

def process_data(spark):
    # Пример чтения файла из HDFS
    hdfs_path = "hdfs://namenode:8020/datamarts/DataMart_transaction/ddl/schema.sql"
    logger.info(f"Чтение файла: {hdfs_path}")
    
    try:
        content = spark.sparkContext.textFile(hdfs_path).collect()
        logger.info(f"Содержимое файла ({len(content)} строк):")
        for line in content[:10]:  # Выводим первые 10 строк
            logger.info(line)
    except Exception as e:
        logger.error(f"Ошибка при чтении файла: {str(e)}")
        raise

if __name__ == "__main__":
    main()

# from config.spark_session import SparkSessionManager
# from utils.logger import SparkLogger
# from steps.preload import Preload
# import argparse



# def main():
#     args = SparkSessionManager.parse_arguments()
#     config = SparkSessionManager.get_spark_config(args.task_id)
    
#     # Создаем экземпляр менеджера и работаем с сессией
#     spark_manager = SparkSessionManager(config)
#     spark = spark_manager.start_session()

#     # Инициализация логгера
#     logger = SparkLogger(env=args.env, step_name=args.step)
#     logger.print_header()


#     try:
#         # Создаем новую сессию для каждой задачи
#         spark = spark_manager.start_session()

#         hdfs_file_path = "/datamarts/DataMart_transaction/ddl/schema.sql"
#         file_content = spark.sparkContext.textFile(hdfs_file_path).collect()
#         for line in file_content:
#             print(line)
            
#         if args.step == 'stage_preload':
#             logger.print_step_info("stage_preload")
#             # Preload.run_and_save_sql_hdfs(spark, args.query_path, args.query_mapping, args.table_schema, args.table_name, args.repartition, args.partition_by, args.bucket_by, args.num_buckets,
#             #                               args.location, args.do_truncate_table, args.do_drop_table, args.do_msck_repair_table, args.temp_view_name, args.cache_df)
        


#         elif args.step == 'stage_calc_stg':
#             logger.print_step_info("stage_calc_stg")

#         elif args.step == 'stage_check_stg':
#             logger.print_step_info("stage_check_stg")

#         elif args.step == 'stage_calc_inc':
#             logger.print_step_info("stage_calc_inc")
          
#         elif args.step == 'stage_check_inc':
#             logger.print_step_info("stage_check_inc")

#         elif args.step == 'stage_MTP':
#             logger.print_step_info("stage_MTP")
            
#         elif args.step == 'stage_hist':
#             logger.print_step_info("stage_hist")
            
#         elif args.step == 'final_check':
#             logger.print_step_info("final_check")
            
#     except Exception as e:
#         print(f"Error in task {args.task_id}: {str(e)}")
#         raise

#     finally:
#         spark_manager.stop_session()
#         logger.print_stats()

# if __name__ == "__main__":
#     main()