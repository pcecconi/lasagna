"""
Simplified unit tests for InputManager class
"""

import pytest
import sys
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession

# Add src to path for imports
sys.path.insert(0, '/usr/local/spark_dev/work/payments_pipelines/src')
from payments_pipeline.common.input_manager import InputManager


class TestInputManagerSimple:
    """Simplified test cases for InputManager class"""
    
    @pytest.fixture
    def spark_session(self):
        """Create a Spark session for testing"""
        return SparkSession.builder \
            .appName("InputManagerTest") \
            .master("local[2]") \
            .getOrCreate()
    
    @pytest.fixture
    def input_manager(self, spark_session):
        """Create InputManager instance for testing"""
        return InputManager(
            spark_session=spark_session,
            s3_status_prefix="s3a://test-bucket/status/",
            pipeline_name="test_pipeline"
        )
    
    def test_init(self, spark_session):
        """Test InputManager initialization"""
        manager = InputManager(
            spark_session=spark_session,
            s3_status_prefix="s3a://test-bucket/status/",
            pipeline_name="test_pipeline"
        )
        
        assert manager.spark == spark_session
        assert manager.s3_status_prefix == "s3a://test-bucket/status/"
        assert manager.pipeline_name == "test_pipeline"
    
    def test_init_normalizes_s3_prefix(self, spark_session):
        """Test that S3 prefix is normalized (adds trailing slash)"""
        manager = InputManager(
            spark_session=spark_session,
            s3_status_prefix="s3a://test-bucket/status",  # No trailing slash
            pipeline_name="test_pipeline"
        )
        
        assert manager.s3_status_prefix == "s3a://test-bucket/status/"
    
    def test_mark_files_as_processed_mismatched_lengths(self, input_manager):
        """Test error when filenames and row_counts have different lengths"""
        filenames = ["file1.csv", "file2.csv"]
        row_counts = [100]  # Different length
        
        with pytest.raises(ValueError, match="filenames and row_counts must have the same length"):
            input_manager.mark_files_as_processed(filenames, row_counts)
    
    def test_load_data_from_file_empty_file(self, input_manager):
        """Test error when no file provided"""
        with pytest.raises(ValueError, match="No file provided for loading"):
            input_manager.load_data_from_file("")
    
    @patch('pyspark.sql.SparkSession.read')
    def test_get_uploaded_files_exception_handling(self, mock_read, input_manager):
        """Test that exceptions are properly handled and return empty list"""
        # Mock exception during read
        mock_format = Mock()
        mock_format.load.side_effect = Exception("S3 error")
        mock_read.return_value.format.return_value = mock_format
        
        files = input_manager.get_uploaded_files()
        assert files == []
    
    @patch('pyspark.sql.SparkSession.read')
    def test_has_processing_marker_exception_handling(self, mock_read, input_manager):
        """Test that exceptions during marker check return False"""
        # Mock exception during read
        mock_format = Mock()
        mock_format.load.side_effect = Exception("S3 error")
        mock_read.return_value.format.return_value = mock_format
        
        result = input_manager.has_processing_marker("test_file.csv")
        assert result is False
    
    def test_get_new_files_to_process_with_mocks(self, input_manager):
        """Test getting new files to process with mocked dependencies"""
        # Mock uploaded files
        with patch.object(input_manager, 'get_uploaded_files', return_value=[
            "merchants_2024-01-01.csv",
            "merchants_2024-01-02.csv"
        ]):
            # Mock processing marker checks
            with patch.object(input_manager, 'has_processing_marker', side_effect=[False, True]):
                files = input_manager.get_new_files_to_process(
                    file_pattern="merchants_",
                    s3_data_prefix="s3a://warehouse/payments/"
                )
                
                # Should only return files without processing markers
                assert len(files) == 1
                assert files[0] == "s3a://warehouse/payments/merchants_2024-01-01.csv"
    
    def test_get_new_files_to_process_no_s3_prefix(self, input_manager):
        """Test getting new files to process without S3 data prefix"""
        # Mock uploaded files
        with patch.object(input_manager, 'get_uploaded_files', return_value=[
            "s3a://warehouse/payments/merchants_2024-01-01.csv"
        ]):
            # Mock processing marker check
            with patch.object(input_manager, 'has_processing_marker', return_value=False):
                files = input_manager.get_new_files_to_process()
                
                assert len(files) == 1
                assert files[0] == "s3a://warehouse/payments/merchants_2024-01-01.csv"
    
    def test_get_processing_stats_basic(self, input_manager):
        """Test getting processing statistics with basic mocking"""
        # Mock uploaded files
        with patch.object(input_manager, 'get_uploaded_files', return_value=[
            "file1.csv", "file2.csv", "file3.csv"
        ]):
            # Mock processing marker checks - all return False for simplicity
            with patch.object(input_manager, 'has_processing_marker', return_value=False):
                stats = input_manager.get_processing_stats()
                
                assert stats["total_uploaded"] == 3
                assert stats["processed"] == 0
                assert stats["pending"] == 3
                assert stats["total_rows_processed"] == 0
