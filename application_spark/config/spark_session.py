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
            "master": "yarn",
            "spark_configs": {
                "spark.executor.memory": "1g",
                "spark.driver.memory": "1g",
                "spark.default.parallelism": "8",
                "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020",
                "spark.submit.deployMode": "client",
                "spark.yarn.resourcemanager.hostname": "namenode",
                "spark.yarn.resourcemanager.address": "namenode:8032",
                "spark.yarn.resourcemanager.scheduler.address": "namenode:8030",
                "spark.yarn.historyServer.address": "namenode:18080",
                "spark.hadoop.yarn.resourcemanager.hostname": "namenode",
                "spark.hadoop.dfs.client.use.datanode.hostname": "true",
                "spark.hadoop.dfs.replication": "1"
            }
        }
        return config
    
    @staticmethod
    def parse_arguments():
        parser = argparse.ArgumentParser()
        parser.add_argument('--env', required=True)
        parser.add_argument('--step', required=True)
        parser.add_argument('--datamart', required=True)
        parser.add_argument('--task-id', required=True)
        parser.add_argument('--query_mapping', required=True)
        parser.add_argument('--query_path', required=True)
        parser.add_argument('--table_schema', required=True)
        parser.add_argument('--table_name', required=True)
        parser.add_argument('--repartition', required=True)
        parser.add_argument('--partition_by', required=True)
        parser.add_argument('--bucket_by', required=True)
        parser.add_argument('--num_buckets', required=True)
        parser.add_argument('--location', required=True)
        parser.add_argument('--do_truncate_table', required=True)
        parser.add_argument('--do_drop_table', required=True)
        parser.add_argument('--do_msck_repair_table', required=True)
        parser.add_argument('--temp_view_name', required=True)
        parser.add_argument('--cache_df', required=True)
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