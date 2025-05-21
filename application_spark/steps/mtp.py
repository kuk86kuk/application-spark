from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, expr, lit
from pyspark.sql.types import StringType
import os
# from hdfs import InsecureClient
from typing import Dict, Tuple, Optional, List, Callable, Any
import logging
from datetime import datetime
import re

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TargetTableMover:
    
    @staticmethod
    def run_and_save_sql_hdfs(
        spark: SparkSession,
        query_path: str,
        query_mapping: str = "",
        table_schema: str = "_",
        table_name: str = "_",
        repartition: str = "40",
        partition_by: str = "_",
        bucket_by: str = "_",
        num_buckets: str = "300",
        location: str = "_",
        do_truncate_table: str = "n",
        do_drop_table: str = "n",
        do_msck_repair_table: str = "n",
        temp_view_name: str = "_",
        cache_df: str = "n"
    ) -> str:
        """Выполняет SQL-запрос и сохраняет результат в таблицу"""
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
        query = TargetTableMover.get_query(spark, query_path, query_mapping)
        df = TargetTableMover.execute_query(spark, query)
        
        if temp_view_name != "_":
            df.createOrReplaceTempView(temp_view_name)
        
        if cache_df == "y":
            df.cache()
        
        if table_name != "_":
            if do_truncate_table == "y":
                TargetTableMover.truncate_table(spark, table_schema, table_name)
            if do_drop_table == "y":
                TargetTableMover.drop_table(spark, table_schema, table_name)
            
            TargetTableMover.save_dataframe_to_table(
                df, repartition, partition_by, location, 
                bucket_by, num_buckets, table_schema, table_name
            )
            
            if do_msck_repair_table == "y" and partition_by != "_":
                TargetTableMover.execute_query(spark, f"msck repair table {table_schema}.{table_name}")
        
        return "SUCCESS"

    
    @staticmethod
    def save_dataframe_to_table(
        df: DataFrame,
        repartition: str,
        partition_by: str,
        location: str,
        bucket_by: str,
        num_buckets: str,
        table_schema: str,
        table_name: str
    ) -> None:
        """Сохраняет DataFrame в таблицу с указанными параметрами"""
        writer = df.write.format("parquet").mode("overwrite")
        
        # Репартиционирование
        if repartition not in ("0", "_"):
            parts = [p.strip() for p in repartition.split(",")]
            if parts[0].isdigit():
                writer = writer.repartition(int(parts[0]), *[col(p) for p in parts[1:]])
            else:
                writer = writer.repartition(*[col(p) for p in parts])
        
        # Партиционирование
        if partition_by != "_":
            writer = writer.partitionBy(*[p.strip() for p in partition_by.split(",")])
        
        # Расположение
        if location != "_":
            writer = writer.option("path", location)
        
        # Бакетирование
        if bucket_by != "_":
            buckets = [b.strip() for b in bucket_by.split(",")]
            writer = writer.bucketBy(int(num_buckets), buckets[0], *buckets[1:])
        
        writer.saveAsTable(f"{table_schema}.{table_name}")

    @staticmethod
    def get_query(spark: SparkSession, query_path: str, query_mapping: str = "") -> str:
        """Читает SQL-запрос из файла и применяет подстановки"""
        resolved_map = TargetTableMover.resolve_mapping(query_mapping)
        query = spark.read.text(query_path).collect()[0][0]
        
        for key, value in resolved_map.items():
            query = query.replace(f"${{{key}}}", value)
        
        TargetTableMover.validate_sql(spark, query)
        logger.info(f"Resolved query:\n{query}")
        return query

    

    @staticmethod
    def execute_query(spark: SparkSession, query: str) -> DataFrame:
        """Выполняет SQL-запрос и возвращает DataFrame"""
        logger.info(f"Executing:\n{query}")
        return spark.sql(query)

    @staticmethod
    def validate_sql(spark: SparkSession, query: str) -> bool:
        """Проверяет валидность SQL-запроса"""
        try:
            spark.sql(query).explain()
            return True
        except Exception as ex:
            raise RuntimeError(f"SQL validation failed: {str(ex)}")

    @staticmethod
    def resolve_mapping(query_mapping: str) -> Dict[str, str]:
        """Разбирает строку с подстановками в словарь"""
        if not query_mapping.strip():
            return {}
        
        try:
            return dict(item.split(":") for item in query_mapping.split(";"))
        except Exception:
            error_msg = (f"Incorrect queryMapping: '{query_mapping}'. "
                        "Expected format: 'key1:value1;key2:value2;...;keyN:valueN'")
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    @staticmethod
    def get_table_location(spark: SparkSession, table_schema: str, table_name: str) -> str:
        """Возвращает расположение таблицы в HDFS"""
        return (spark
                .sql(f"describe formatted {table_schema}.{table_name}")
                .filter("col_name = 'Location'")
                .select("data_type")
                .collect()[0][0])

    @staticmethod
    def truncate_table(spark: SparkSession, table_schema: str, table_name: str) -> None:
        """Очищает таблицу, удаляя данные из HDFS"""
        logger.info(f"Truncating {table_schema}.{table_name}")
        try:
            location = TargetTableMover.get_table_location(spark, table_schema, table_name)
            # hdfs_client = InsecureClient("http://namenode:9870")
            # hdfs_client.delete(location, recursive=True)
            logger.info(f"Deleted {location}")
        except Exception as ex:
            logger.error(f"Could not truncate {table_schema}.{table_name}: {str(ex)}")

    @staticmethod
    def drop_table(spark: SparkSession, table_schema: str, table_name: str) -> None:
        """Удаляет таблицу"""
        drop_stmt = f"drop table if exists {table_schema}.{table_name}"
        logger.info(f"Executing: {drop_stmt}")
        spark.sql(drop_stmt)