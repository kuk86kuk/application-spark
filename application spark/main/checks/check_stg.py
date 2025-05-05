from SparkManager import SparkManager
from pyspark.sql import functions as F
from typing import Dict, List, Tuple
import logging

class StagingValidator:
    def __init__(self, env: str, params: Dict):
        self.spark = SparkManager.get_spark(env)
        self.params = params
        self.logger = logging.getLogger(self.__class__.__name__)
        self.results = {
            'passed': True,
            'checks': []
        }

    def _log_check(self, name: str, passed: bool, details: str = ""):
        """Log check result and add to summary"""
        result = {
            'check': name,
            'status': 'PASSED' if passed else 'FAILED',
            'details': details
        }
        self.results['checks'].append(result)
        if not passed:
            self.results['passed'] = False
        
        msg = f"{name}: {'PASSED' if passed else 'FAILED'}"
        if details:
            msg += f" | {details}"
        self.logger.info(msg)

    def _check_table_exists(self, table_name: str) -> bool:
        """Verify that table exists in catalog"""
        try:
            if not self.spark.catalog.tableExists(table_name):
                raise ValueError(f"Table {table_name} does not exist")
            return True
        except Exception as e:
            self._log_check("Table Existence", False, str(e))
            raise

    def _check_not_null(self, df, columns: List[str]) -> Dict[str, Tuple[bool, int]]:
        """Check for NULL values in specified columns"""
        results = {}
        for col_name in columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            results[col_name] = (null_count == 0, null_count)
        return results

    def _check_unique(self, df, columns: List[str]) -> bool:
        """Check uniqueness of primary key"""
        total_count = df.count()
        distinct_count = df.select(columns).distinct().count()
        return total_count == distinct_count

    def _check_value_ranges(self, df, column_rules: Dict) -> Dict[str, bool]:
        """Validate value ranges/rules for columns"""
        results = {}
        for col_name, rules in column_rules.items():
            query = None
            if 'min' in rules:
                query = (F.col(col_name) >= rules['min']) if query is None else query & (F.col(col_name) >= rules['min'])
            if 'max' in rules:
                query = (F.col(col_name) <= rules['max']) if query is None else query & (F.col(col_name) <= rules['max'])
            if 'allowed_values' in rules:
                query = (F.col(col_name).isin(rules['allowed_values'])) if query is None else query & (F.col(col_name).isin(rules['allowed_values']))
            
            invalid_count = df.filter(~query).count() if query is not None else 0
            results[col_name] = (invalid_count == 0, invalid_count)
        
        return results

    def _check_foreign_keys(self, df, fk_rules: List[Dict]) -> Dict[str, bool]:
        """Validate foreign key relationships"""
        results = {}
        for rule in fk_rules:
            parent_df = self.spark.table(rule['parent_table'])
            parent_ids = parent_df.select(rule['parent_column']).distinct()
            
            # Find IDs in child table not present in parent
            invalid_ids = df.join(
                parent_ids, 
                df[rule['child_column']] == parent_ids[rule['parent_column']], 
                'left_anti'
            )
            
            invalid_count = invalid_ids.count()
            check_name = f"FK_{rule['child_column']}_to_{rule['parent_table']}.{rule['parent_column']}"
            results[check_name] = (invalid_count == 0, invalid_count)
        
        return results

    def validate(self) -> Dict:
        """Execute all validation checks"""
        try:
            table_name = self.params['table_name']
            self._check_table_exists(table_name)
            
            df = self.spark.table(table_name)
            pk_columns = self.params['primary_key']
            not_null_columns = self.params.get('not_null_columns', [])
            value_rules = self.params.get('value_rules', {})
            fk_rules = self.params.get('foreign_keys', [])
            
            # 1. Проверка первичного ключа
            pk_unique = self._check_unique(df, pk_columns)
            self._log_check(
                "Primary Key Uniqueness", 
                pk_unique,
                f"Columns: {', '.join(pk_columns)}"
            )
            
            # 2. Проверка на NULL в PK и других важных колонках
            not_null_results = self._check_not_null(df, pk_columns + not_null_columns)
            for col, (passed, count) in not_null_results.items():
                self._log_check(
                    f"NULL Check - {col}",
                    passed,
                    f"NULL count: {count}" if not passed else ""
                )
            
            # 3. Проверка допустимых значений
            value_check_results = self._check_value_ranges(df, value_rules)
            for col, (passed, count) in value_check_results.items():
                self._log_check(
                    f"Value Check - {col}",
                    passed,
                    f"Invalid values: {count}" if not passed else ""
                )
            
            # 4. Проверка внешних ключей
            fk_results = self._check_foreign_keys(df, fk_rules)
            for fk, (passed, count) in fk_results.items():
                self._log_check(
                    f"Foreign Key - {fk}",
                    passed,
                    f"Invalid references: {count}" if not passed else ""
                )
            
            # 5. Проверка объема данных (опционально)
            if 'expected_min_rows' in self.params:
                row_count = df.count()
                passed = row_count >= self.params['expected_min_rows']
                self._log_check(
                    "Minimum Row Count",
                    passed,
                    f"Expected ≥ {self.params['expected_min_rows']}, got {row_count}"
                )
            
            return self.results
            
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            raise

def process_check_stg(**kwargs):
    """Airflow task function for staging validation"""
    params = kwargs['params']['check_stg']
    env = kwargs['params']['environment']
    
    try:
        validator = StagingValidator(env, params)
        validation_result = validator.validate()
        
        if not validation_result['passed']:
            failed_checks = [c for c in validation_result['checks'] if c['status'] == 'FAILED']
            error_msg = f"{len(failed_checks)} checks failed:\n" + \
                       "\n".join(f"{c['check']}: {c['details']}" for c in failed_checks)
            raise ValueError(error_msg)
            
        return validation_result
        
    except Exception as e:
        logging.error(f"STG validation failed: {str(e)}")
        raise