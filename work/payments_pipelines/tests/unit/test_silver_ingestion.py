"""
Unit tests for SilverIngestionJob class - TDD approach
"""

import pytest
from unittest.mock import Mock, patch
from datetime import date

from payments_pipeline.silver.silver_ingestion import SilverIngestionJob


class TestSilverIngestionJob:
    """Test cases for SilverIngestionJob class"""
    
    def test_init(self, test_config):
        """Test SilverIngestionJob initialization"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        # Act
        with patch('payments_data_source.silver.silver_ingestion.setup_logging', return_value=mock_logger):
            job = SilverIngestionJob(test_config, mock_spark)
        
        # Assert
        assert job.config == test_config
        assert job.spark == mock_spark
        assert job.logger == mock_logger
        assert job.atomic_updater is not None
        assert job.data_quality_checker is not None
    
    def test_should_have_pipeline_orchestration_methods(self, test_config):
        """Test that the silver layer has pipeline orchestration functionality"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        with patch('payments_data_source.silver.silver_ingestion.setup_logging', return_value=mock_logger):
            job = SilverIngestionJob(test_config, mock_spark)
            
            # Assert
            assert hasattr(job, 'run_complete_silver_pipeline'), "Should have complete pipeline method"
            assert hasattr(job, 'run_incremental_update'), "Should have incremental update method"
            assert hasattr(job, 'run_full_reprocess'), "Should have full reprocess method"
            assert hasattr(job, 'get_data_quality_results'), "Should have data quality results method"
            assert hasattr(job, 'get_silver_layer_stats'), "Should have statistics method"
    
    def test_should_initialize_components_correctly(self, test_config):
        """Test that the silver layer initializes all required components"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        with patch('payments_data_source.silver.silver_ingestion.setup_logging', return_value=mock_logger):
            job = SilverIngestionJob(test_config, mock_spark)
            
            # Assert
            assert job.atomic_updater is not None, "Should initialize atomic updater"
            assert job.data_quality_checker is not None, "Should initialize data quality checker"
            assert hasattr(job.atomic_updater, 'create_silver_tables'), "Atomic updater should have table creation"
            assert hasattr(job.data_quality_checker, 'run_all_checks'), "Data quality checker should have validation"