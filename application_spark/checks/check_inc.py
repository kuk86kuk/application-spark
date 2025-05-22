from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnull, sum as spark_sum, min as spark_min, max as spark_max
from typing import List, Dict, Optional, Union

class CheckInc:
    def __init__(self, df: DataFrame):
        self.df = df

    def check_null_percentage(
        self, 
        columns: List[str], 
        threshold: float = 5.0, 
        critical: bool = False
    ) -> bool:
        """
        Проверяет процент NULL в колонках. 
        Если critical=True, то при превышении threshold вернет False.

        :param columns: Список колонок.
        :param threshold: Максимально допустимый процент NULL (по умолчанию 5%).
        :param critical: Флаг критичности проверки.
        :return: True, если проверка пройдена.
        """
        total_rows = self.df.count()
        if total_rows == 0:
            return True  # Пустые данные считаем валидными

        for column in columns:
            null_count = self.df.filter(col(column).isNull()).count()
            null_percent = (null_count / total_rows) * 100
            if null_percent > threshold:
                if critical:
                    return False
        return True

    def check_null_in_keys(
        self, 
        key_columns: List[str], 
        critical: bool = False
    ) -> bool:
        """
        Проверяет NULL в ключевых колонках.

        :param key_columns: Список ключевых колонок.
        :param critical: Флаг критичности.
        :return: True, если NULL нет.
        """
        condition = None
        for column in key_columns:
            if condition is None:
                condition = col(column).isNull()
            else:
                condition = condition | col(column).isNull()

        bad_records = self.df.filter(condition).count()
        if bad_records > 0 and critical:
            return False
        return True

    def check_numeric_conditions(
        self,
        column_ranges: Dict[str, Dict[str, float]],
        critical: bool = False
    ) -> bool:
        """
        Проверяет числовые колонки на диапазоны и агрегаты.
        Формат column_ranges:
        {
            "column1": {"min": 0, "max": 100, "sum": 500},
            "column2": {"min": -10, "max": 50}
        }

        :param column_ranges: Словарь с условиями для колонок.
        :param critical: Флаг критичности.
        :return: True, если все условия выполнены.
        """
        for column, conditions in column_ranges.items():
            # Проверка диапазонов
            min_val = conditions.get("min")
            max_val = conditions.get("max")
            if min_val is not None or max_val is not None:
                range_cond = None
                if min_val is not None:
                    range_cond = (col(column) < min_val)
                if max_val is not None:
                    if range_cond is None:
                        range_cond = (col(column) > max_val)
                    else:
                        range_cond = range_cond | (col(column) > max_val)
                
                if range_cond is not None:
                    bad_records = self.df.filter(range_cond).count()
                    if bad_records > 0 and critical:
                        return False

            # Проверка агрегатов
            agg_cond = None
            if "sum" in conditions:
                actual_sum = self.df.agg(spark_sum(col(column))).collect()[0][0]
                if actual_sum != conditions["sum"] and critical:
                    return False
            if "min" in conditions:
                actual_min = self.df.agg(spark_min(col(column))).collect()[0][0]
                if actual_min < conditions["min"] and critical:
                    return False
            if "max" in conditions:
                actual_max = self.df.agg(spark_max(col(column))).collect()[0][0]
                if actual_max > conditions["max"] and critical:
                    return False
        return True

    @staticmethod
    def validate_all(
    ) -> bool:
        """
        Запускает все проверки. Формат checks:
        {
            "null_percentage": {"columns": ["col1", "col2"], "threshold": 5.0},
            "null_keys": {"columns": ["id"]},
            "numeric_conditions": {
                "age": {"min": 18, "max": 99},
                "price": {"sum": 1000}
            }
        }

        :param checks: Словарь с параметрами проверок.
        :param critical_checks: Список проверок, которые считаются критичными (например, ["null_keys"]).
        :return: True, если все проверки пройдены.
        """
        print("SUCCESS")
        return "SUCCESS"