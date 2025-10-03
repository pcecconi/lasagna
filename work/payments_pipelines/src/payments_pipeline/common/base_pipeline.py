#!/usr/bin/env python3
"""
Base Pipeline Classes

Provides base classes and common functionality for all pipeline types.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame

from ..utils.config import PipelineConfig
from ..utils.logging import setup_logging


@dataclass
class PipelineResult:
    """Result of a pipeline execution"""
    success: bool
    message: str
    metrics: Dict[str, Any]
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    
    def __post_init__(self):
        if self.start_time and self.end_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()


class BasePipeline(ABC):
    """
    Base class for all pipeline implementations
    
    Provides common functionality:
    - Configuration management
    - Spark session handling
    - Logging setup
    - Error handling
    - Metrics collection
    """
    
    def __init__(self, 
                 config: Optional[PipelineConfig] = None, 
                 spark_session: Optional[SparkSession] = None,
                 pipeline_name: str = None):
        self.config = config or PipelineConfig()
        self.pipeline_name = pipeline_name or self.__class__.__name__
        self.logger = setup_logging(f"{__name__}.{self.pipeline_name}")
        
        # Use provided Spark session or get from utils
        if spark_session:
            self.spark = spark_session
        else:
            from ..utils.spark import get_spark_session
            self.spark = get_spark_session(self.pipeline_name)
        
        # Pipeline state
        self._start_time: Optional[datetime] = None
        self._metrics: Dict[str, Any] = {}
    
    @abstractmethod
    def execute(self) -> PipelineResult:
        """
        Execute the pipeline
        
        Returns:
            PipelineResult: Result of the pipeline execution
        """
        pass
    
    def validate_inputs(self) -> bool:
        """
        Validate pipeline inputs
        
        Returns:
            bool: True if inputs are valid, False otherwise
        """
        try:
            self.logger.info("🔍 Validating pipeline inputs...")
            # Override in subclasses for specific validation
            self.logger.info("✅ Input validation passed")
            return True
        except Exception as e:
            self.logger.error(f"❌ Input validation failed: {e}")
            return False
    
    def rollback(self) -> bool:
        """
        Rollback pipeline changes
        
        Returns:
            bool: True if rollback succeeded, False otherwise
        """
        try:
            self.logger.info("🔄 Rolling back pipeline changes...")
            # Override in subclasses for specific rollback logic
            self.logger.info("✅ Rollback completed")
            return True
        except Exception as e:
            self.logger.error(f"❌ Rollback failed: {e}")
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get pipeline execution metrics
        
        Returns:
            Dict[str, Any]: Pipeline metrics
        """
        return {
            "pipeline_name": self.pipeline_name,
            "start_time": self._start_time,
            "metrics": self._metrics.copy()
        }
    
    def _start_execution(self):
        """Start pipeline execution and record start time"""
        self._start_time = datetime.now()
        self.logger.info(f"🚀 Starting {self.pipeline_name}")
    
    def _end_execution(self, success: bool, message: str = "") -> PipelineResult:
        """End pipeline execution and create result"""
        end_time = datetime.now()
        
        result = PipelineResult(
            success=success,
            message=message,
            metrics=self._metrics.copy(),
            start_time=self._start_time,
            end_time=end_time,
            duration_seconds=0  # Will be calculated in __post_init__
        )
        
        status_emoji = "✅" if success else "❌"
        self.logger.info(f"{status_emoji} {self.pipeline_name} completed in {result.duration_seconds:.2f}s")
        
        return result
    
    def _record_metric(self, name: str, value: Any):
        """Record a metric"""
        self._metrics[name] = value
    
    def _handle_error(self, error: Exception, context: str = "") -> PipelineResult:
        """Handle pipeline errors consistently"""
        error_msg = f"Pipeline failed{f' in {context}' if context else ''}: {str(error)}"
        self.logger.error(error_msg)
        
        # Attempt rollback
        try:
            self.rollback()
        except Exception as rollback_error:
            self.logger.error(f"Rollback also failed: {rollback_error}")
        
        return self._end_execution(False, error_msg)


class DataIngestionMixin:
    """
    Mixin for data ingestion pipelines
    
    Provides common functionality for ingesting data from various sources
    """
    
    def add_metadata(self, df: DataFrame, source_path: str, layer: str) -> DataFrame:
        """
        Add standard metadata columns to a DataFrame
        
        Args:
            df: Source DataFrame
            source_path: Source file path
            layer: Data layer (bronze, silver, gold)
            
        Returns:
            DataFrame with metadata columns
        """
        from pyspark.sql.functions import current_timestamp, lit, input_file_name
        
        return df.withColumn("ingestion_timestamp", current_timestamp()) \
                 .withColumn("source_file", input_file_name()) \
                 .withColumn(f"{layer}_layer_version", lit("1.0")) \
                 .withColumn("data_source", lit("payments_generator"))
    
    def validate_schema(self, df: DataFrame, expected_columns: List[str]) -> bool:
        """
        Validate DataFrame schema
        
        Args:
            df: DataFrame to validate
            expected_columns: List of expected column names
            
        Returns:
            bool: True if schema is valid, False otherwise
        """
        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            self.logger.error(f"❌ Missing required columns: {missing_columns}")
            return False
        
        self.logger.info("✅ Schema validation passed")
        return True
    
    def handle_data_errors(self, df: DataFrame, error_config: Dict[str, Any]) -> DataFrame:
        """
        Handle common data errors
        
        Args:
            df: DataFrame to process
            error_config: Configuration for error handling
            
        Returns:
            DataFrame with errors handled
        """
        from pyspark.sql.functions import when, col, isnan, isnull
        
        # Handle null values based on config
        if error_config.get("handle_nulls", False):
            for column, default_value in error_config.get("null_defaults", {}).items():
                if column in df.columns:
                    df = df.withColumn(
                        column,
                        when(col(column).isNull(), default_value).otherwise(col(column))
                    )
        
        # Handle NaN values
        if error_config.get("handle_nan", False):
            for column in error_config.get("nan_columns", []):
                if column in df.columns:
                    df = df.withColumn(
                        column,
                        when(isnan(col(column)), None).otherwise(col(column))
                    )
        
        return df


class TableManagementMixin:
    """
    Mixin for table management operations
    
    Provides common functionality for creating and managing Iceberg tables
    """
    
    def create_namespace(self, namespace: str):
        """Create Iceberg namespace if it doesn't exist"""
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.config.iceberg_catalog}.{namespace}")
            self.logger.info(f"✅ Namespace {namespace} created/verified")
        except Exception as e:
            self.logger.warning(f"⚠️ Namespace creation warning: {e}")
    
    def drop_table(self, table_name: str):
        """Drop table if it exists"""
        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            self.logger.info(f"✅ Table {table_name} dropped")
        except Exception as e:
            self.logger.warning(f"⚠️ Table drop warning: {e}")
    
    def verify_table(self, table_name: str) -> bool:
        """Verify table exists and is accessible"""
        try:
            self.spark.table(table_name).count()
            self.logger.info(f"✅ Table {table_name} verified")
            return True
        except Exception as e:
            self.logger.error(f"❌ Table {table_name} verification failed: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table information and statistics"""
        try:
            df = self.spark.table(table_name)
            
            info = {
                "table_name": table_name,
                "row_count": df.count(),
                "column_count": len(df.columns),
                "columns": df.columns,
                "schema": [{"name": field.name, "type": str(field.dataType)} for field in df.schema.fields]
            }
            
            return info
            
        except Exception as e:
            self.logger.error(f"❌ Error getting table info: {e}")
            return {"table_name": table_name, "error": str(e)}


class DataQualityMixin:
    """
    Mixin for data quality operations
    
    Provides common data quality validation functionality
    """
    
    def validate_required_columns(self, df: DataFrame, required_columns: List[str]) -> Dict[str, Any]:
        """Validate that required columns exist"""
        missing_columns = set(required_columns) - set(df.columns)
        
        return {
            "validation_type": "required_columns",
            "required_columns": required_columns,
            "existing_columns": df.columns,
            "missing_columns": list(missing_columns),
            "passed": len(missing_columns) == 0
        }
    
    def validate_null_values(self, df: DataFrame, key_columns: List[str]) -> Dict[str, Any]:
        """Validate null values in key columns"""
        from pyspark.sql.functions import col
        
        null_counts = {}
        for col_name in key_columns:
            if col_name in df.columns:
                null_counts[col_name] = df.filter(col(col_name).isNull()).count()
        
        return {
            "validation_type": "null_values",
            "key_columns": key_columns,
            "null_counts": null_counts,
            "passed": all(count == 0 for count in null_counts.values())
        }
    
    def validate_row_count(self, df: DataFrame, min_rows: int = 1, max_rows: int = None) -> Dict[str, Any]:
        """Validate row count is within expected range"""
        row_count = df.count()
        
        passed = row_count >= min_rows
        if max_rows is not None:
            passed = passed and row_count <= max_rows
        
        return {
            "validation_type": "row_count",
            "actual_count": row_count,
            "min_rows": min_rows,
            "max_rows": max_rows,
            "passed": passed
        }
    
    def validate_duplicates(self, df: DataFrame, key_columns: List[str]) -> Dict[str, Any]:
        """Validate no duplicate records based on key columns"""
        from pyspark.sql.functions import col
        
        duplicate_count = df.groupBy(*key_columns).count().filter(col("count") > 1).count()
        
        return {
            "validation_type": "duplicates",
            "key_columns": key_columns,
            "duplicate_count": duplicate_count,
            "passed": duplicate_count == 0
        }
