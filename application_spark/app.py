from config.spark_session import SparkSessionManager
from utils.logger import SparkLogger
from steps.preload import Preload
import argparse



def main():
    args = SparkSessionManager.parse_arguments()
    config = SparkSessionManager.get_spark_config(args.task_id)
    
    # Создаем экземпляр менеджера и работаем с сессией
    spark_manager = SparkSessionManager(config)
    spark = spark_manager.start_session()

    # Инициализация логгера
    logger = SparkLogger(env=args.env, step_name=args.step)
    logger.print_header()


    try:
        # Создаем новую сессию для каждой задачи
        spark = spark_manager.start_session()

        if args.step == 'stage_preload':
            logger.print_step_info("stage_preload")
            
            hdfs_file_path = "hdfs://namenode:8020/datamarts/DataMart_transaction/ddl/schema.sql"
            file_content = spark.sparkContext.textFile(hdfs_file_path).collect()
            for line in file_content:
                print(line)
                
            # Preload.run_and_save_sql_hdfs(spark, args.query_path, args.query_mapping, args.table_schema, args.table_name, args.repartition, args.partition_by, args.bucket_by, args.num_buckets,
            #                               args.location, args.do_truncate_table, args.do_drop_table, args.do_msck_repair_table, args.temp_view_name, args.cache_df)
        


        elif args.step == 'stage_calc_stg':
            logger.print_step_info("stage_calc_stg")

        elif args.step == 'stage_check_stg':
            logger.print_step_info("stage_check_stg")

        elif args.step == 'stage_calc_inc':
            logger.print_step_info("stage_calc_inc")
          
        elif args.step == 'stage_check_inc':
            logger.print_step_info("stage_check_inc")

        elif args.step == 'stage_MTP':
            logger.print_step_info("stage_MTP")
            
        elif args.step == 'stage_hist':
            logger.print_step_info("stage_hist")
            
        elif args.step == 'final_check':
            logger.print_step_info("final_check")
            
    except Exception as e:
        print(f"Error in task {args.task_id}: {str(e)}")
        raise

    finally:
        spark_manager.stop_session()
        logger.print_stats()

if __name__ == "__main__":
    main()