from SparkManager import SparkManager
from pyspark.sql import functions as F
from typing import Dict
import logging

class TargetTableMover:
    def __init__(self, env: str, params: Dict):
        self.spark = SparkManager.get_spark(env)
        self.params = params
        self.logger = logging.getLogger(self.__class__.__name__)
        self.merge_stats = {}

    def _validate_input(self):
        """Validate input parameters"""
        required_params = ['target_table', 'increment_table', 'merge_keys']
        for param in required_params:
            if param not in self.params:
                raise ValueError(f"Missing required parameter: {param}")

    def _get_merge_condition(self, df_target, df_increment):
        """Generate merge condition based on merge keys"""
        conditions = []
        for key in self.params['merge_keys']:
            if key not in df_increment.columns or key not in df_target.columns:
                raise ValueError(f"Merge key {key} not found in both tables")
            conditions.append(f"target.{key} = increment.{key}")
        return " AND ".join(conditions)

    def _prepare_increment_data(self):
        """Prepare increment data with optional transformations"""
        df = self.spark.table(self.params['increment_table'])
        
        # Apply filter if specified
        if 'increment_filter' in self.params:
            df = df.filter(self.params['increment_filter'])
            
        # Select specific columns if specified
        if 'columns' in self.params:
            df = df.select(*self.params['columns'])
            
        return df

    def _merge_data(self, df_target, df_increment):
        """Perform MERGE operation (UPSERT pattern)"""
        merge_condition = self._get_merge_condition(df_target, df_increment)
        target_table = self.params['target_table']
        temp_view = "increment_data"
        
        df_increment.createOrReplaceTempView(temp_view)
        
        # Build merge columns (all columns except merge keys)
        merge_columns = [c for c in df_increment.columns if c not in self.params['merge_keys']]
        
        # Generate SQL for merge
        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {temp_view} AS increment
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET {', '.join([f"target.{col} = increment.{col}" for col in merge_columns])}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(df_increment.columns)})
                VALUES ({', '.join([f"increment.{col}" for col in df_increment.columns])})
        """
        
        self.logger.info(f"Executing MERGE operation:\n{merge_sql}")
        self.spark.sql(merge_sql)
        
        # Get merge statistics
        self.merge_stats = {
            'target_table': target_table,
            'increment_table': self.params['increment_table'],
            'merge_keys': self.params['merge_keys'],
            'merged_at': str(F.current_timestamp())
        }

    def _cleanup_increment(self):
        """Clean up increment data after successful merge"""
        if self.params.get('cleanup_increment', True):
            self.logger.info(f"Cleaning up increment table: {self.params['increment_table']}")
            self.spark.sql(f"TRUNCATE TABLE {self.params['increment_table']}")
            self.merge_stats['increment_cleaned'] = True
        else:
            self.merge_stats['increment_cleaned'] = False

    def execute(self):
        """Main method to move data from increment to target"""
        self._validate_input()
        
        df_target = self.spark.table(self.params['target_table'])
        df_increment = self._prepare_increment_data()
        
        initial_count = df_target.count()
        increment_count = df_increment.count()
        
        self.logger.info(f"Starting merge: {increment_count} records to merge into {initial_count} existing records")
        
        self._merge_data(df_target, df_increment)
        self._cleanup_increment()
        
        final_count = self.spark.table(self.params['target_table']).count()
        self.merge_stats.update({
            'initial_count': initial_count,
            'increment_count': increment_count,
            'final_count': final_count,
            'records_changed': final_count - initial_count
        })
        
        self.logger.info(f"Merge completed. Stats: {self.merge_stats}")
        return self.merge_stats

def process_move_to_target(**kwargs):
    """Airflow task function for moving data to target"""
    params = kwargs['params']['move_to_target']
    env = kwargs['params']['environment']
    
    try:
        mover = TargetTableMover(env, params)
        result = mover.execute()
        
        # Можно добавить дополнительные проверки результата
        if result['increment_count'] == 0:
            logging.warning("No data in increment table")
            
        return result
        
    except Exception as e:
        logging.error(f"Failed to move data to target: {str(e)}")
        raise