"""
Spark Session Management

Handles Spark session creation and configuration for the payments pipeline.
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType


def get_spark_session(
    app_name: str = "PaymentsPipeline",
    master: str = "spark://spark-master:7077",
    config: Optional[dict] = None
) -> SparkSession:
    """
    Create and configure Spark session for payments pipeline
    
    Args:
        app_name: Spark application name
        master: Spark master URL
        config: Additional Spark configuration
        
    Returns:
        Configured SparkSession
    """
    
    # Default configuration (only configs that can be set after session creation)
    default_config = {
        # Iceberg configuration
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "hive",
        "spark.sql.catalog.iceberg.uri": "thrift://hive-metastore:9083",
        
        # Performance optimization
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        
        # S3/MinIO configuration
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "admin",
        "spark.hadoop.fs.s3a.secret.key": "password",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        
        # Warehouse directory
        "spark.sql.warehouse.dir": "s3a://warehouse/",
    }
    
    # Merge with provided config
    if config:
        default_config.update(config)
    
    # Create Spark session builder
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .enableHiveSupport()
    
    # Apply configuration during session creation
    for key, value in default_config.items():
        builder = builder.config(key, value)
    
    # Create the session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logging.info(f"✅ Spark session created: {app_name}")
    logging.info(f"   Master: {master}")
    logging.info(f"   Spark version: {spark.version}")
    
    return spark


def get_payments_schemas() -> dict:
    """
    Get predefined schemas for payments data
    
    Returns:
        Dictionary of schemas for different data types
    """
    
    merchant_schema = StructType([
        StructField("merchant_id", StringType(), False),
        StructField("merchant_name", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("mdr_rate", DoubleType(), True),
        StructField("size_category", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("last_transaction_date", StringType(), True),
    ])
    
    transaction_schema = StructType([
        StructField("payment_id", StringType(), False),
        StructField("payment_timestamp", StringType(), True),
        StructField("payment_lat", DoubleType(), True),
        StructField("payment_lng", DoubleType(), True),
        StructField("payment_amount", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("terminal_id", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("card_issuer", StringType(), True),
        StructField("card_brand", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("transactional_cost_rate", DoubleType(), True),
        StructField("transactional_cost_amount", DoubleType(), True),
        StructField("mdr_amount", DoubleType(), True),
        StructField("net_profit", DoubleType(), True),
    ])
    
    return {
        "merchant": merchant_schema,
        "transaction": transaction_schema
    }


def stop_spark_session(spark: SparkSession):
    """
    Stop Spark session gracefully
    
    Args:
        spark: SparkSession to stop
    """
    try:
        spark.stop()
        logging.info("✅ Spark session stopped")
    except Exception as e:
        logging.error(f"❌ Error stopping Spark session: {e}")


def test_spark_connectivity(spark: SparkSession) -> bool:
    """
    Test Spark connectivity and configuration
    
    Args:
        spark: SparkSession to test
        
    Returns:
        True if connectivity is successful
    """
    try:
        # Test basic Spark operations
        test_df = spark.range(10)
        count = test_df.count()
        assert count == 10
        
        # Test Iceberg catalog
        spark.sql("SHOW NAMESPACES IN iceberg").show()
        
        # Test S3 connectivity
        spark.sql("SHOW TABLES IN iceberg").show()
        
        logging.info("✅ Spark connectivity test passed")
        return True
        
    except Exception as e:
        logging.error(f"❌ Spark connectivity test failed: {e}")
        return False
