from config.spark_session import SparkSessionManager
from utils.logger import SparkLogger
from steps import calc_inc, calc_stg, mtp, preload
from checks import check_inc, check_stg, final_checks
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
        print(args.step)   
        if args.step == 'preload':
            logger.print_step_info("stage_preload")
            preload.run_and_save_sql_hdfs(spark, 
        args.query_path,
        args.query_mapping,
        args.table_schema,
        args.table_name,
        args.repartition,
        args.partition_by,
        args.bucket_by,
        args.num_buckets,
        args.location,
        args.do_truncate_table,
        args.do_drop_table,
        args.do_msck_repair_table,
        args.temp_view_name,
        args.cache_df,)

        # elif args.step == 'calc_stg':
        #     logger.print_step_info("stage_calc_stg")
        #     calc_stg.run_and_save_sql_hdfs()

        # elif args.step == 'check_stg':
        #     logger.print_step_info("stage_check_stg")
        #     check_stg.validate_all()

        # elif args.step == 'calc_inc':
        #     logger.print_step_info("stage_calc_inc")
        #     calc_inc.run_and_save_sql_hdfs()
          
        # elif args.step == 'check_inc':
        #     logger.print_step_info("stage_check_inc")
        #     check_inc.validate_all()

        # elif args.step == 'MTP':
        #     logger.print_step_info("stage_MTP")
        #     mtp.run_and_save_sql_hdfs()
            
        # elif args.step == 'hist':
        #     logger.print_step_info("stage_hist")
        #     calc_hist.run_and_save_sql_hdfs()
            
        # elif args.step == 'final_check':
        #     logger.print_step_info("final_check")
        #     final_checks.validate_all()
            
    except Exception as e:
        print(f"Error in task {args.task_id}: {str(e)}")
        raise

    finally:
        spark_manager.stop_session()
        logger.print_stats()

if __name__ == "__main__":
    main()