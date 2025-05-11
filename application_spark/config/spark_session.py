from pyspark.sql import SparkSession

class SparkSessionManager:
    def __init__(self, config):
        self.config = config
        self.spark = None

    def start_session(self):
        builder = SparkSession.builder \
            .appName(self.config["app_name"]) \
            .master(self.config["master"])

        # Применяем дополнительные конфигурации
        for key, value in self.config["spark_configs"].items():
            builder = builder.config(key, value)

        self.spark = builder.getOrCreate()
        return self.spark

    def stop_session(self):
        if self.spark:
            self.spark.stop()
            self.spark = None