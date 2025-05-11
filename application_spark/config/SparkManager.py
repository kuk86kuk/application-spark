from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging
from typing import Dict, Any

class SparkManager:
    _sessions = {}  # Class-level storage for Spark sessions

    @classmethod
    def init_spark(cls, env: str, config: Dict[str, Any] = None):
        """Initialize Spark session for specific environment"""
        if env in cls._sessions:
            return cls._sessions[env]
        
        try:
            conf = SparkConf()
            conf.set("spark.app.name", f"datamart_pipeline_{env}")
            conf.set("spark.sql.session.timeZone", "UTC")
            
            # Apply custom config if provided
            if config:
                for k, v in config.items():
                    conf.set(k, str(v))
            
            master = 'yarn' if env == 'prod' else 'local[*]'
            
            spark = SparkSession.builder \
                .master(master) \
                .config(conf=conf) \
                .enableHiveSupport() \
                .getOrCreate()
            
            cls._sessions[env] = spark
            logging.info(f"Spark session initialized for {env} environment")
            return spark
            
        except Exception as e:
            logging.error(f"Failed to initialize Spark: {str(e)}")
            raise

    @classmethod
    def get_spark(cls, env: str):
        """Get existing Spark session"""
        if env not in cls._sessions:
            raise RuntimeError(f"Spark session for {env} not initialized")
        return cls._sessions[env]

    @classmethod
    def stop(cls, env: str):
        """Stop Spark session"""
        if env in cls._sessions:
            try:
                spark = cls._sessions.pop(env)
                spark.stop()
                logging.info(f"Spark session for {env} stopped")
            except Exception as e:
                logging.error(f"Error stopping Spark: {str(e)}")
                raise