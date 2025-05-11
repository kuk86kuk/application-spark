from config.spark_session import SparkSessionManager
import logging
import argparse

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_config(step):
    return {
        "app_name": f"MySparkApp_{step}",
        "master": "spark://spark-master:7077",
        "spark_configs": {
            "spark.executor.memory": "4g",
            "spark.driver.memory": "2g",
            "spark.default.parallelism": "8",
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020"
        }
    }

def run_step(step, datamart=None):
    """Выполняет конкретный этап обработки"""
    config = get_spark_config(step)
    spark_manager = SparkSessionManager(config)
    
    try:
        spark = spark_manager.start_session()
        logger.info(f"Starting {step} step")
        
        if step == "start":
            logger.info("Initialization completed")
            print("Initialization completed")
            
        elif step == "process":
            logger.info(f"Processing datamart: {datamart}")
            print(f"Processing datamart: {datamart}")
            # Здесь основная логика обработки
            
        elif step == "finish":
            logger.info("Finalizing operations")
            print("Finalizing operations")
            
    finally:
        spark_manager.stop_session()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    parser.add_argument("--step", required=True)
    parser.add_argument("--datamart")
    args = parser.parse_args()
    
    run_step(args.step, args.datamart)

if __name__ == "__main__":
    main()

# import argparse
# from datetime import datetime
# import sys
# from steps import preload, calc_stg, calc_inc, mtp, hist
# from checks import check_stg, check_inc, final_checks
# from config import SparkManager

# # Константы для оформления
# BORDER = "=" * 60
# SUB_BORDER = "-" * 50
# HEADER = """
#   _____  _    _ ______ _____  
#  |  __ \| |  | |  ____|  __ \ 
#  | |  | | |  | | |__  | |__) |
#  | |  | | |  | |  __| |  ___/ 
#  | |__| | |__| | |____| |     
#  |_____/ \____/|______|_|     
# """

# def print_header(step_name: str, env: str):
#     """Красивое отображение заголовка этапа"""
#     timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     print(BORDER)
#     print(HEADER)
#     print(f"\n>>> {step_name.upper()} <<<")
#     print(f"Environment: {env}")
#     print(f"Timestamp: {timestamp}")
#     print(BORDER)

# def print_step_info(step: str, action: str):
#     """Информация о выполняемом шаге"""
#     print(f"\n{SUB_BORDER}")
#     print(f"STAGE: {step}")
#     print(f"ACTION: {action}")
#     print(f"{SUB_BORDER}\n")

# def print_stats(start_time: datetime):
#     """Вывод статистики выполнения"""
#     duration = datetime.now() - start_time
#     print(f"\n{SUB_BORDER}")
#     print(f"EXECUTION TIME: {duration.total_seconds():.2f} sec")
#     print(f"STATUS: COMPLETED")
#     print(SUB_BORDER)

# def main():
#     parser = argparse.ArgumentParser(description='ETL Pipeline for Data Marts')
#     parser.add_argument('--env', required=True, 
#                       help='Environment (dev/test/prod)')
#     parser.add_argument('--step', required=True,
#                       help='Execution step (start/final/stage_*)')
#     parser.add_argument('--datamart', required=True,
#                       help='Name of the datamart being processed')
    
#     args = parser.parse_args()
#     global name_datamart
#     name_datamart = args.datamart

#     # Инициализационное и финальное логирование
#     if args.step == 'start':
#         print_header("INITIALIZING DATAMART PIPELINE", args.env)
#         print(f"\nProcessing datamart: {name_datamart}")
#         print("\nInitial checks and setup...")
#         SparkManager.get_spark(args.env)
#         print("\nInitialization completed successfully!")
#         sys.exit(0)
        
#     elif args.step == 'final':
#         print_header("FINALIZING DATAMART PIPELINE", args.env)
#         print(f"\nFinalizing datamart: {name_datamart}")
#         SparkManager.stop(args.env)
#         print("\nPipeline completed successfully!")
#         sys.exit(0)

#     def execute_step():
#         start_time = datetime.now()
        
#         if args.step == 'stage_preload':
#             print_header("DATA PRELOAD STAGE", args.env)
#             print_step_info("PRELOAD", "Loading source data")
#             preload.run(args.env)
            
#         elif args.step == 'stage_calc_stg':
#             print_header("STAGING CALCULATION", args.env)
#             print_step_info("STG CALCULATION", "Building staging layer")
#             calc_stg.run(args.env)
            
#         elif args.step == 'stage_check_stg':
#             print_header("STAGING VALIDATION", args.env)
#             print_step_info("STG VALIDATION", "Quality checks")
#             check_stg.run(args.env)
            
#         elif args.step == 'stage_calc_inc':
#             print_header("INCREMENT CALCULATION", args.env)
#             print_step_info("INCREMENT", "Processing delta changes")
#             calc_inc.run(args.env)
            
#         elif args.step == 'stage_check_inc':
#             print_header("INCREMENT VALIDATION", args.env)
#             print_step_info("INCREMENT CHECK", "Business logic validation")
#             check_inc.run(args.env)
            
#         elif args.step == 'stage_MTP':
#             print_header("MART TABLE POPULATION", args.env)
#             print_step_info("MTP", "Loading mart tables")
#             mtp.run(args.env)
            
#         elif args.step == 'stage_hist':
#             print_header("HISTORY UPDATE", args.env)
#             print_step_info("HISTORY", "Updating historical data")
#             hist.run(args.env)
            
#         elif args.step == 'final_check':
#             print_header("FINAL VALIDATION", args.env)
#             print_step_info("FINAL CHECK", "Comprehensive data validation")
#             final_checks.run(args.env)
            
#         else:
#             raise ValueError(f"Unknown step: {args.step}")
        
#         print_stats(start_time)

#     execute_step()

# if __name__ == "__main__":
#     main()
