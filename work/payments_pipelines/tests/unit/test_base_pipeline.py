#!/usr/bin/env python3
"""
Unit tests for base pipeline classes and mixins
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from payments_pipeline.common.base_pipeline import (
    BasePipeline, 
    DataIngestionMixin, 
    TableManagementMixin, 
    DataQualityMixin,
    PipelineResult
)


class TestPipelineResult:
    """Test PipelineResult dataclass"""
    
    def test_pipeline_result_creation(self):
        """Test PipelineResult creation and duration calculation"""
        start_time = datetime(2024, 1, 1, 10, 0, 0)
        end_time = datetime(2024, 1, 1, 10, 0, 30)
        
        result = PipelineResult(
            success=True,
            message="Test completed",
            metrics={"rows_processed": 1000},
            start_time=start_time,
            end_time=end_time,
            duration_seconds=0  # Will be calculated in __post_init__
        )
        
        assert result.success is True
        assert result.message == "Test completed"
        assert result.metrics["rows_processed"] == 1000
        assert result.duration_seconds == 30.0


class TestBasePipeline:
    """Test BasePipeline abstract class"""
    
    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session"""
        return Mock()
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration"""
        config = Mock()
        config.get = Mock(side_effect=lambda key, default=None: {
            "namespace": "test_namespace",
            "catalog": "test_catalog"
        }.get(key, default))
        return config
    
    def test_base_pipeline_initialization(self, mock_spark, mock_config):
        """Test BasePipeline initialization"""
        with patch('payments_pipeline.utils.spark.get_spark_session') as mock_get_spark:
            mock_get_spark.return_value = mock_spark
            
            # Create concrete implementation for testing
            class TestPipeline(BasePipeline):
                def execute(self):
                    return self._end_execution(True, "Test completed")
            
            pipeline = TestPipeline(config=mock_config, spark_session=mock_spark)
            
            assert pipeline.config == mock_config
            assert pipeline.spark == mock_spark
            assert pipeline.pipeline_name == "TestPipeline"
    
    def test_pipeline_metrics_recording(self, mock_spark, mock_config):
        """Test metric recording functionality"""
        class TestPipeline(BasePipeline):
            def execute(self):
                self._record_metric("test_metric", 42)
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=mock_spark)
        result = pipeline.execute()
        
        assert result.metrics["test_metric"] == 42
    
    def test_pipeline_error_handling(self, mock_spark, mock_config):
        """Test error handling in pipeline execution"""
        class TestPipeline(BasePipeline):
            def execute(self):
                raise ValueError("Test error")
        
        pipeline = TestPipeline(config=mock_config, spark_session=mock_spark)
        
        # The error should propagate since BasePipeline doesn't catch it
        with pytest.raises(ValueError, match="Test error"):
            pipeline.execute()


class TestDataIngestionMixin:
    """Test DataIngestionMixin functionality"""
    
    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session"""
        return Mock()
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration"""
        return {"test": "config"}
    
    def test_add_metadata(self, spark, mock_config):
        """Test metadata addition to DataFrame"""
        # Create a class that uses the mixin
        class TestPipeline(BasePipeline, DataIngestionMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=spark)
        
        # Create real DataFrame
        data = [("1", "Alice"), ("2", "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])
        
        result_df = pipeline.add_metadata(df, "/test/path", "bronze")
        
        # Verify metadata columns were added
        assert "ingestion_timestamp" in result_df.columns
        assert "source_file" in result_df.columns
        assert "bronze_layer_version" in result_df.columns
        assert "data_source" in result_df.columns
    
    def test_validate_schema(self, mock_spark, mock_config):
        """Test schema validation"""
        class TestPipeline(BasePipeline, DataIngestionMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=mock_spark)
        
        # Mock DataFrame with required columns
        mock_df = Mock()
        mock_df.columns = ["id", "name", "value"]
        
        result = pipeline.validate_schema(mock_df, ["id", "name"])
        assert result is True
        
        # Test with missing columns
        result = pipeline.validate_schema(mock_df, ["id", "missing"])
        assert result is False
    
    def test_handle_data_errors(self, spark, mock_config):
        """Test data error handling"""
        class TestPipeline(BasePipeline, DataIngestionMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=spark)
        
        # Create real DataFrame with nulls
        data = [("1", "Alice"), (None, "Bob"), ("3", None)]
        df = spark.createDataFrame(data, ["id", "name"])
        
        error_config = {
            "handle_nulls": True,
            "null_defaults": {"id": "unknown", "name": "unknown"}
        }
        
        result_df = pipeline.handle_data_errors(df, error_config)
        
        # Verify the result is a DataFrame
        assert result_df is not None
        assert hasattr(result_df, 'columns')


class TestTableManagementMixin:
    """Test TableManagementMixin functionality"""
    
    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session"""
        return Mock()
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration"""
        return {"catalog": "test_catalog"}
    
    def test_create_namespace(self, mock_spark, mock_config):
        """Test namespace creation"""
        class TestPipeline(BasePipeline, TableManagementMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        # Create a config with iceberg_catalog attribute
        class MockConfig:
            def __init__(self):
                self.iceberg_catalog = "test_catalog"
        
        pipeline = TestPipeline(config=MockConfig(), spark_session=mock_spark)
        
        pipeline.create_namespace("test_namespace")
        
        # Verify SQL was executed
        mock_spark.sql.assert_called_with("CREATE NAMESPACE IF NOT EXISTS test_catalog.test_namespace")
    
    def test_drop_table(self, mock_spark, mock_config):
        """Test table dropping"""
        class TestPipeline(BasePipeline, TableManagementMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=mock_spark)
        
        pipeline.drop_table("test_table")
        
        mock_spark.sql.assert_called_with("DROP TABLE IF EXISTS test_table")
    
    def test_verify_table(self, mock_spark, mock_config):
        """Test table verification"""
        class TestPipeline(BasePipeline, TableManagementMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=mock_spark)
        
        # Mock successful table access
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_spark.table.return_value = mock_df
        
        result = pipeline.verify_table("test_table")
        
        assert result is True
        mock_spark.table.assert_called_with("test_table")
    
    def test_get_table_info(self, mock_spark, mock_config):
        """Test table info retrieval"""
        class TestPipeline(BasePipeline, TableManagementMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=mock_spark)
        
        # Mock DataFrame with schema
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.columns = ["col1", "col2"]
        
        mock_field1 = Mock()
        mock_field1.name = "col1"
        mock_field1.dataType = "StringType"
        
        mock_field2 = Mock()
        mock_field2.name = "col2"
        mock_field2.dataType = "IntegerType"
        
        mock_df.schema.fields = [mock_field1, mock_field2]
        mock_spark.table.return_value = mock_df
        
        info = pipeline.get_table_info("test_table")
        
        assert info["table_name"] == "test_table"
        assert info["row_count"] == 1000
        assert info["column_count"] == 2
        assert info["columns"] == ["col1", "col2"]


class TestDataQualityMixin:
    """Test DataQualityMixin functionality"""
    
    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session"""
        return Mock()
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration"""
        return {"test": "config"}
    
    def test_validate_required_columns(self, mock_spark, mock_config):
        """Test required columns validation"""
        class TestPipeline(BasePipeline, DataQualityMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=mock_spark)
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.columns = ["id", "name", "value"]
        
        result = pipeline.validate_required_columns(mock_df, ["id", "name"])
        
        assert result["validation_type"] == "required_columns"
        assert result["passed"] is True
        assert result["missing_columns"] == []
        
        # Test with missing columns
        result = pipeline.validate_required_columns(mock_df, ["id", "missing"])
        assert result["passed"] is False
        assert "missing" in result["missing_columns"]
    
    def test_validate_null_values(self, spark, mock_config):
        """Test null values validation"""
        class TestPipeline(BasePipeline, DataQualityMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=spark)
        
        # Create real DataFrame with no nulls
        data = [("1", "Alice"), ("2", "Bob"), ("3", "Charlie")]
        df = spark.createDataFrame(data, ["id", "name"])
        
        result = pipeline.validate_null_values(df, ["id", "name"])
        
        assert result["validation_type"] == "null_values"
        assert result["passed"] is True
        assert result["null_counts"]["id"] == 0
        assert result["null_counts"]["name"] == 0
    
    def test_validate_row_count(self, mock_spark, mock_config):
        """Test row count validation"""
        class TestPipeline(BasePipeline, DataQualityMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=mock_spark)
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000
        
        result = pipeline.validate_row_count(mock_df, min_rows=100, max_rows=2000)
        
        assert result["validation_type"] == "row_count"
        assert result["passed"] is True
        assert result["actual_count"] == 1000
        
        # Test with count outside range
        result = pipeline.validate_row_count(mock_df, min_rows=2000)
        assert result["passed"] is False
    
    def test_validate_duplicates(self, spark, mock_config):
        """Test duplicate validation"""
        class TestPipeline(BasePipeline, DataQualityMixin):
            def execute(self):
                return self._end_execution(True, "Test completed")
        
        pipeline = TestPipeline(config=mock_config, spark_session=spark)
        
        # Create real DataFrame with no duplicates
        data = [("1", "Alice"), ("2", "Bob"), ("3", "Charlie")]
        df = spark.createDataFrame(data, ["id", "name"])
        
        result = pipeline.validate_duplicates(df, ["id"])
        
        assert result["validation_type"] == "duplicates"
        assert result["passed"] is True
        assert result["duplicate_count"] == 0


if __name__ == "__main__":
    pytest.main([__file__])
