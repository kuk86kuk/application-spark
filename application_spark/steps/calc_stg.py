from config.SparkManager import SparkManager
from pyspark.sql import DataFrame
import logging
from typing import Dict
import re

class StagingCalculator:
    def __init__(self, env: str, params: Dict):
        self.spark = SparkManager.get_spark(env)
        self.params = params
        self.logger = logging.getLogger(self.__class__.__name__)

    def _read_sql_file(self, hdfs_path: str) -> str:
        """Read SQL file from HDFS"""
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            Path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path
            FileSystem = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem
            
            fs = FileSystem.get(hadoop_conf)
            path = Path(hdfs_path)
            
            if not fs.exists(path):
                raise FileNotFoundError(f"SQL file not found: {hdfs_path}")
            
            stream = fs.open(path)
            sql_content = ""
            try:
                sql_content = "".join([line.decode('utf-8') for line in stream])
            finally:
                stream.close()
                
            return sql_content
            
        except Exception as e:
            self.logger.error(f"Failed to read SQL file: {str(e)}")
            raise

    def _replace_parameters(self, sql: str) -> str:
        """Replace placeholders in SQL with actual parameters"""
        try:
            # Стандартные параметры
            replacements = {
                '${source1}': self.params['source1'],
                '${source2}': self.params['source2'],
                '${processing_date}': self.params['processing_date'],
                '${env}': self.params['environment']
            }
            
            # Дополнительные параметры из конфига
            for param, value in self.params.get('sql_params', {}).items():
                placeholder = f'${{{param}}}'
                replacements[placeholder] = value
            
            # Заменяем все плейсхолдеры
            for placeholder, value in replacements.items():
                sql = sql.replace(placeholder, str(value))
                
            return sql
            
        except Exception as e:
            self.logger.error(f"Parameter replacement failed: {str(e)}")
            raise

    def _validate_sql(self, sql: str) -> None:
        """Check for unresolved parameters"""
        unresolved = set(re.findall(r'\$\{\w+\}', sql))
        if unresolved:
            raise ValueError(f"Unresolved parameters in SQL: {unresolved}")

    def execute(self) -> DataFrame:
        """Main method to execute parameterized SQL"""
        try:
            # 1. Получаем SQL-файл
            sql_file_path = self.params['sql_file_path']
            self.logger.info(f"Reading SQL file from: {sql_file_path}")
            raw_sql = self._read_sql_file(sql_file_path)
            
            # 2. Подставляем параметры
            self.logger.info("Applying parameters to SQL")
            processed_sql = self._replace_parameters(raw_sql)
            self._validate_sql(processed_sql)
            
            # 3. Выполняем SQL
            self.logger.info("Executing SQL query")
            return self.spark.sql(processed_sql)
            
        except Exception as e:
            self.logger.error(f"STG calculation failed: {str(e)}")
            raise

def process_calc_stg(**kwargs):
    """Airflow task function for staging calculation"""
    params = kwargs['params']['calc_stg']
    env = kwargs['params']['environment']
    
    try:
        calculator = StagingCalculator(env, params)
        df = calculator.execute()
        
        # Сохранение результата
        if 'output_path' in params:
            SparkManager.write_with_partitioning(
                df=df,
                output_path=params['output_path'],
                partition_cols=params.get('partition_cols', []),
                bucket_cols=params.get('bucket_cols', []),
                num_buckets=params.get('num_buckets')
            )
            logging.info(f"STG results saved to {params['output_path']}")
            
        return True
        
    except Exception as e:
        logging.error(f"STG processing failed: {str(e)}")
        raise