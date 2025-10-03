#!/usr/bin/env python3
"""
Pytest configuration and shared fixtures
"""

import pytest
from unittest.mock import Mock
from pathlib import Path
import tempfile
import shutil
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestPipeline") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()
    
    yield spark
    
    # Cleanup
    spark.stop()


@pytest.fixture
def spark(spark_session):
    """Alias for spark_session fixture"""
    return spark_session


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests"""
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path)


@pytest.fixture
def mock_spark():
    """Mock Spark session"""
    spark = Mock()
    spark.sql = Mock()
    spark.table = Mock()
    spark.read = Mock()
    return spark


@pytest.fixture
def mock_dataframe():
    """Mock DataFrame"""
    df = Mock()
    df.columns = ["id", "name", "value"]
    df.count = Mock(return_value=1000)
    df.filter = Mock(return_value=df)
    df.groupBy = Mock(return_value=df)
    df.withColumn = Mock(return_value=df)
    df.select = Mock(return_value=df)
    
    # Mock schema
    mock_field1 = Mock()
    mock_field1.name = "id"
    mock_field1.dataType = "StringType"
    mock_field2 = Mock()
    mock_field2.name = "name"
    mock_field2.dataType = "StringType"
    mock_field3 = Mock()
    mock_field3.name = "value"
    mock_field3.dataType = "IntegerType"
    
    df.schema.fields = [mock_field1, mock_field2, mock_field3]
    
    return df


@pytest.fixture
def sample_config():
    """Sample configuration for testing"""
    return {
        "version": "1.0",
        "description": "Test Configuration",
        "pipeline_groups": {
            "bronze_layer": {
                "description": "Bronze layer pipelines",
                "enabled": True,
                "pipelines": {
                    "bronze_merchants": {
                        "class_name": "BronzeMerchantsPipeline",
                        "dependencies": [],
                        "config": {
                            "table_name": "merchants_raw",
                            "namespace": "payments_bronze",
                            "catalog": "iceberg"
                        },
                        "enabled": True,
                        "description": "Ingest raw merchant data"
                    }
                }
            }
        },
        "global_config": {
            "spark_config": {
                "app_name": "TestPipeline",
                "master": "local[*]"
            }
        }
    }


@pytest.fixture
def cleanup_spark_artifacts():
    """Fixture to clean up Spark artifacts after tests"""
    temp_views = []
    temp_tables = []
    
    def register_temp_view(view_name):
        temp_views.append(view_name)
    
    def register_temp_table(table_name):
        temp_tables.append(table_name)
    
    yield register_temp_view, register_temp_table
    
    # Cleanup after test
    # Note: In real Spark environment, you would clean up temp views and tables here
    # For now, we just track them for documentation
    if temp_views:
        print(f"Test created temp views: {temp_views}")
    if temp_tables:
        print(f"Test created temp tables: {temp_tables}")