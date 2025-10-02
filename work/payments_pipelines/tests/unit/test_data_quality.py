"""
Unit tests for DataQualityChecker class - TDD approach
"""

import pytest
from unittest.mock import Mock, patch

from payments_pipeline.silver.data_quality import DataQualityChecker


class TestDataQualityChecker:
    """Test cases for DataQualityChecker class"""
    
    def test_init(self, test_config):
        """Test DataQualityChecker initialization"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        # Act
        with patch('payments_data_source.silver.data_quality.setup_logging', return_value=mock_logger):
            checker = DataQualityChecker(mock_spark)
        
        # Assert
        assert checker.spark == mock_spark
        assert checker.logger == mock_logger
    
    def test_should_run_comprehensive_data_quality_checks(self, test_config):
        """Test that the silver layer can run comprehensive data quality validation"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        mock_df = Mock()
        mock_spark.sql.return_value = mock_df
        mock_df.collect.return_value = [(1000, 1000, 0)]  # All checks pass
        
        with patch('payments_data_source.silver.data_quality.setup_logging', return_value=mock_logger):
            checker = DataQualityChecker(mock_spark)
            
            # Act
            result = checker.run_all_checks()
            
            # Assert
            assert mock_spark.sql.called, "Should execute SQL for data quality checks"
            assert result is not None, "Should return data quality results"
            assert "summary" in result, "Should include summary of checks"
    
    def test_should_have_data_quality_check_methods(self, test_config):
        """Test that the silver layer has comprehensive data quality validation methods"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        with patch('payments_data_source.silver.data_quality.setup_logging', return_value=mock_logger):
            checker = DataQualityChecker(mock_spark)
            
            # Assert
            assert hasattr(checker, '_check_transaction_completeness'), "Should have transaction completeness check"
            assert hasattr(checker, '_check_merchant_completeness'), "Should have merchant completeness check"
            assert hasattr(checker, '_check_referential_integrity'), "Should have referential integrity check"
            assert hasattr(checker, '_check_data_consistency'), "Should have data consistency check"
            assert hasattr(checker, '_check_duplicates'), "Should have duplicate check"
            assert hasattr(checker, 'generate_report'), "Should have report generation method"
    
    def test_should_have_comprehensive_quality_checks(self, test_config):
        """Test that the silver layer can run comprehensive data quality validation"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        mock_df = Mock()
        mock_spark.sql.return_value = mock_df
        mock_df.collect.return_value = [(1000, 1000, 0)]  # All checks pass
        
        with patch('payments_data_source.silver.data_quality.setup_logging', return_value=mock_logger):
            checker = DataQualityChecker(mock_spark)
            
            # Act
            result = checker.run_all_checks()
            
            # Assert
            assert mock_spark.sql.called, "Should execute SQL for data quality checks"
            assert result is not None, "Should return data quality results"
            assert "summary" in result, "Should include summary of checks"