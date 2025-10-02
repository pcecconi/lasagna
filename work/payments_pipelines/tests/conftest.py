"""
Pytest configuration and fixtures for payments pipeline tests
"""

import pytest
import uuid
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock

from pyspark.sql import SparkSession

# Add src to path for imports
import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from payments_pipeline.utils.config import PipelineConfig
from payments_pipeline.utils.spark import get_spark_session


@pytest.fixture(scope="session")
def test_database():
    """Create a single test database for the entire test suite"""
    test_db = f"test_silver_{uuid.uuid4().hex[:8]}"
    spark = get_spark_session()
    spark.sql(f"CREATE DATABASE {test_db}")
    
    yield test_db
    
    # Teardown: Drop all tables first, then database
    try:
        # Drop all tables in the database
        tables = spark.sql(f"SHOW TABLES IN {test_db}").collect()
        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {test_db}.{table.tableName}")
        
        # Drop the database
        spark.sql(f"DROP DATABASE {test_db}")
    except Exception as e:
        print(f"Warning: Error during cleanup: {e}")
    finally:
        spark.stop()


@pytest.fixture
def test_config(test_database):
    """Create test configuration with the test database"""
    return PipelineConfig(
        iceberg_catalog="spark_catalog",
        silver_namespace=test_database,
        bronze_namespace=test_database
    )


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


