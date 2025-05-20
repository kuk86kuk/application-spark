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
        print(args.step)
        hdfs_file_path = "hdfs://namenode:8020/datamarts/DataMart_transaction/ddl/schema.sql"
        print(hdfs_file_path)
        file_content = spark.sparkContext.textFile(hdfs_file_path).collect()
        print(file_content)
        for line in file_content:
            print(line)
            
        if args.step == 'preload':
            logger.print_step_info("stage_preload")


        elif args.step == 'calc_stg':
            logger.print_step_info("stage_calc_stg")

        elif args.step == 'check_stg':
            logger.print_step_info("stage_check_stg")

        elif args.step == 'calc_inc':
            logger.print_step_info("stage_calc_inc")
          
        elif args.step == 'check_inc':
            logger.print_step_info("stage_check_inc")

        elif args.step == 'MTP':
            logger.print_step_info("stage_MTP")
            
        elif args.step == 'hist':
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