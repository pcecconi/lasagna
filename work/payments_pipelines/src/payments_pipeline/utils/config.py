"""
Configuration Management

Handles configuration for the payments pipeline.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class PipelineConfig:
    """
    Configuration class for payments pipeline
    """
    
    # Spark configuration
    spark_master: str = "spark://spark-master:7077"
    spark_app_name: str = "PaymentsPipeline"
    
    # Iceberg configuration
    iceberg_catalog: str = "iceberg"
    iceberg_uri: str = "thrift://hive-metastore:9083"
    
    # Storage configuration
    warehouse_dir: str = "s3a://warehouse/"
    bronze_namespace: str = "payments_bronze"
    silver_namespace: str = "payments_silver"
    gold_namespace: str = "payments_gold"
    
    # MinIO/S3 configuration
    s3_endpoint: str = "http://minio:9000"
    s3_access_key: str = "admin"
    s3_secret_key: str = "password"
    
    # Processing configuration
    batch_size: int = 10000
    max_file_age_days: int = 30
    
    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    @classmethod
    def from_file(cls, config_path: str) -> 'PipelineConfig':
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            PipelineConfig instance
        """
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return cls(**config_data)
    
    @classmethod
    def from_env(cls) -> 'PipelineConfig':
        """
        Load configuration from environment variables
        
        Returns:
            PipelineConfig instance
        """
        return cls(
            spark_master=os.getenv("SPARK_MASTER", "spark://spark-master:7077"),
            spark_app_name=os.getenv("SPARK_APP_NAME", "PaymentsPipeline"),
            iceberg_catalog=os.getenv("ICEBERG_CATALOG", "iceberg"),
            iceberg_uri=os.getenv("ICEBERG_URI", "thrift://hive-metastore:9083"),
            warehouse_dir=os.getenv("WAREHOUSE_DIR", "s3a://warehouse/"),
            bronze_namespace=os.getenv("BRONZE_NAMESPACE", "payments_bronze"),
            silver_namespace=os.getenv("SILVER_NAMESPACE", "payments_silver"),
            gold_namespace=os.getenv("GOLD_NAMESPACE", "payments_gold"),
            s3_endpoint=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            s3_access_key=os.getenv("S3_ACCESS_KEY", "admin"),
            s3_secret_key=os.getenv("S3_SECRET_KEY", "password"),
            batch_size=int(os.getenv("BATCH_SIZE", "10000")),
            max_file_age_days=int(os.getenv("MAX_FILE_AGE_DAYS", "30")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary
        
        Returns:
            Configuration dictionary
        """
        return {
            "spark_master": self.spark_master,
            "spark_app_name": self.spark_app_name,
            "iceberg_catalog": self.iceberg_catalog,
            "iceberg_uri": self.iceberg_uri,
            "warehouse_dir": self.warehouse_dir,
            "bronze_namespace": self.bronze_namespace,
            "silver_namespace": self.silver_namespace,
            "gold_namespace": self.gold_namespace,
            "s3_endpoint": self.s3_endpoint,
            "s3_access_key": self.s3_access_key,
            "s3_secret_key": self.s3_secret_key,
            "batch_size": self.batch_size,
            "max_file_age_days": self.max_file_age_days,
            "log_level": self.log_level,
            "log_format": self.log_format,
        }
    
    def get_spark_config(self) -> Dict[str, str]:
        """
        Get Spark-specific configuration
        
        Returns:
            Spark configuration dictionary
        """
        return {
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "hive",
            "spark.sql.catalog.iceberg.uri": self.iceberg_uri,
            "spark.sql.warehouse.dir": self.warehouse_dir,
            "spark.hadoop.fs.s3a.endpoint": self.s3_endpoint,
            "spark.hadoop.fs.s3a.access.key": self.s3_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.s3_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        }
    
    def get_table_names(self) -> Dict[str, str]:
        """
        Get fully qualified table names
        
        Returns:
            Dictionary of table names
        """
        return {
            "merchants_raw": f"{self.iceberg_catalog}.{self.bronze_namespace}.merchants_raw",
            "transactions_raw": f"{self.iceberg_catalog}.{self.bronze_namespace}.transactions_raw",
            "dim_merchants": f"{self.iceberg_catalog}.{self.silver_namespace}.dim_merchants",
            "fact_payments": f"{self.iceberg_catalog}.{self.silver_namespace}.fact_payments",
        }
    
    def save_to_file(self, config_path: str):
        """
        Save configuration to YAML file
        
        Args:
            config_path: Path to save configuration file
        """
        config_path = Path(config_path)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(config_path, 'w') as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False)
    
    def __str__(self) -> str:
        """String representation of configuration"""
        return f"PipelineConfig(app_name={self.spark_app_name}, catalog={self.iceberg_catalog})"


def get_config(config_path: Optional[str] = None) -> PipelineConfig:
    """
    Get configuration from file or environment
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        PipelineConfig instance
    """
    if config_path and Path(config_path).exists():
        return PipelineConfig.from_file(config_path)
    else:
        return PipelineConfig.from_env()

