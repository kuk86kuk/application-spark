from pyspark.sql import SparkSession
import argparse

class SparkSessionManager:
    def __init__(self, config):
        self.config = config
        self.spark = None

    @staticmethod
    def get_spark_config(task_id=None):
        config = {
            "app_name": f"MySparkApp_{task_id}" if task_id else "MySparkApp",
            "master": "spark://spark-master:7077",
            "spark_configs": {
                "spark.executor.memory": "4g",
                "spark.driver.memory": "2g",
                "spark.default.parallelism": "8",
                "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020",
                "spark.submit.deployMode": "client"
            }
        }
        return config
    
    @staticmethod
    def parse_arguments():
        parser = argparse.ArgumentParser()
        parser.add_argument('--query_mapping', required=True)
        parser.add_argument('--datamart', required=True)
        parser.add_argument('--task_id', required=True)
        return parser.parse_args()

    def start_session(self):
        builder = SparkSession.builder \
            .appName(self.config["app_name"]) \
            .master(self.config["master"])

        for key, value in self.config["spark_configs"].items():
            builder = builder.config(key, value)

        self.spark = builder.getOrCreate()
        return self.spark

    def stop_session(self):
        if self.spark:
            self.spark.stop()
            self.spark = None