"""
Unit tests for AtomicSilverUpdater class - TDD approach
"""

import pytest
from unittest.mock import Mock, patch

from payments_pipeline.silver.atomic_updates import AtomicSilverUpdater


class TestAtomicSilverUpdater:
    """Test cases for AtomicSilverUpdater class"""
    
    def test_init(self, test_config):
        """Test AtomicSilverUpdater initialization"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        # Act
        with patch('payments_pipeline.silver.atomic_updates.setup_logging', return_value=mock_logger):
            updater = AtomicSilverUpdater(test_config, mock_spark)
        
        # Assert
        assert updater.config == test_config
        assert updater.spark == mock_spark
        assert updater.logger == mock_logger
    
    def test_create_silver_tables_calls_sql(self, test_config):
        """Test that create_silver_tables calls spark.sql"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        with patch('payments_pipeline.silver.atomic_updates.setup_logging', return_value=mock_logger):
            updater = AtomicSilverUpdater(test_config, mock_spark)
        
        # Act
        updater.create_silver_tables()
        
        # Assert
        assert mock_spark.sql.called
        # Should call SQL at least 3 times (for 3 tables)
        assert mock_spark.sql.call_count >= 3
    
    def test_should_have_merchant_scd_type2_table_structure(self, test_config):
        """Test that the silver layer creates a merchant dimension with SCD Type 2 structure"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        with patch('payments_pipeline.silver.atomic_updates.setup_logging', return_value=mock_logger):
            updater = AtomicSilverUpdater(test_config, mock_spark)
        
        # Act
        updater.create_silver_tables()
        
        # Assert
        assert mock_spark.sql.called
        # Check that dim_merchants table has SCD Type 2 fields
        sql_calls = [call[0][0] for call in mock_spark.sql.call_args_list]
        merchants_sql = [sql for sql in sql_calls if 'dim_merchants' in sql.lower()]
        assert len(merchants_sql) > 0, "Should create dim_merchants table"
        
        merchants_table_sql = merchants_sql[0]
        assert 'effective_date' in merchants_table_sql, "Should have effective_date for SCD Type 2"
        assert 'expiry_date' in merchants_table_sql, "Should have expiry_date for SCD Type 2"
        assert 'is_current' in merchants_table_sql, "Should have is_current for SCD Type 2"
        assert 'merchant_sk' in merchants_table_sql, "Should have surrogate key"
    
    def test_should_have_payment_fact_table_with_merchant_reference(self, test_config):
        """Test that the silver layer creates a payment fact table with merchant reference"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        with patch('payments_pipeline.silver.atomic_updates.setup_logging', return_value=mock_logger):
            updater = AtomicSilverUpdater(test_config, mock_spark)
        
        # Act
        updater.create_silver_tables()
        
        # Assert
        assert mock_spark.sql.called
        # Check that fact_payments table has merchant reference
        sql_calls = [call[0][0] for call in mock_spark.sql.call_args_list]
        payments_sql = [sql for sql in sql_calls if 'fact_payments' in sql.lower()]
        assert len(payments_sql) > 0, "Should create fact_payments table"
        
        payments_table_sql = payments_sql[0]
        assert 'merchant_id' in payments_table_sql, "Should have merchant_id for direct access"
        assert 'merchant_sk' in payments_table_sql, "Should have merchant_sk for dimension join"
        assert 'payment_date' in payments_table_sql, "Should have payment_date for partitioning"
    
    def test_should_have_date_dimension_with_business_calendar_fields(self, test_config):
        """Test that the silver layer creates a date dimension with business calendar fields"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        with patch('payments_pipeline.silver.atomic_updates.setup_logging', return_value=mock_logger):
            updater = AtomicSilverUpdater(test_config, mock_spark)
        
        # Act
        updater.create_silver_tables()
        
        # Assert
        assert mock_spark.sql.called
        # Check that dim_date table has business calendar fields
        sql_calls = [call[0][0] for call in mock_spark.sql.call_args_list]
        date_sql = [sql for sql in sql_calls if 'dim_date' in sql.lower()]
        assert len(date_sql) > 0, "Should create dim_date table"
        
        date_table_sql = date_sql[0]
        assert 'is_weekend' in date_table_sql, "Should have is_weekend field"
        assert 'is_holiday' in date_table_sql, "Should have is_holiday field"
        assert 'is_business_day' in date_table_sql, "Should have is_business_day field"
        assert 'quarter' in date_table_sql, "Should have quarter field"
        assert 'fiscal_year' in date_table_sql, "Should have fiscal_year field"
    
    def test_should_handle_missing_merchants_gracefully(self, test_config):
        """Test that the silver layer handles payments with missing merchant references"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        mock_df = Mock()
        mock_df.filter.return_value = mock_df
        mock_df.count.return_value = 0  # No missing merchants
        
        with patch('payments_pipeline.silver.atomic_updates.setup_logging', return_value=mock_logger):
            updater = AtomicSilverUpdater(test_config, mock_spark)
        
        # Act
        result = updater._validate_payments_data(mock_df)
        
        # Assert
        assert mock_df.filter.called, "Should filter for missing merchant_sk"
        assert result[0] is True, "Should return validation success"
        assert isinstance(result[1], Mock), "Should return filtered DataFrame"
    
    def test_should_have_merchant_processing_method(self, test_config):
        """Test that the silver layer has merchant processing functionality"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        with patch('payments_pipeline.silver.atomic_updates.setup_logging', return_value=mock_logger):
            updater = AtomicSilverUpdater(test_config, mock_spark)
        
        # Assert
        assert hasattr(updater, 'atomic_update_merchants'), "Should have merchant processing method"
        assert callable(updater.atomic_update_merchants), "Should be callable"
    
    def test_should_have_payment_processing_method(self, test_config):
        """Test that the silver layer has payment processing functionality"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        with patch('payments_pipeline.silver.atomic_updates.setup_logging', return_value=mock_logger):
            updater = AtomicSilverUpdater(test_config, mock_spark)
        
        # Assert
        assert hasattr(updater, 'atomic_update_payments'), "Should have payment processing method"
        assert callable(updater.atomic_update_payments), "Should be callable"
    
    def test_should_have_table_info_method(self, test_config):
        """Test that the silver layer has table metadata functionality"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        
        with patch('payments_pipeline.silver.atomic_updates.setup_logging', return_value=mock_logger):
            updater = AtomicSilverUpdater(test_config, mock_spark)
        
        # Assert
        assert hasattr(updater, 'get_table_info'), "Should have table info method"
        assert callable(updater.get_table_info), "Should be callable"
    
    def test_should_handle_validation_errors_gracefully(self, test_config):
        """Test that the silver layer handles data validation errors without crashing"""
        # Arrange
        mock_spark = Mock()
        mock_logger = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 100  # Simulate validation errors
        mock_df.filter.return_value = mock_df
        
        with patch('payments_pipeline.silver.atomic_updates.setup_logging', return_value=mock_logger), \
             patch('payments_pipeline.silver.atomic_updates.col') as mock_col:
            mock_col.return_value.isNull.return_value = Mock()
            mock_col.return_value.__lt__ = Mock()
            mock_col.return_value.__gt__ = Mock()
            
            updater = AtomicSilverUpdater(test_config, mock_spark)
            
            # Act
            result = updater._validate_merchants_data(mock_df)
            
            # Assert
            assert mock_df.count.called, "Should count records for validation"
            assert result is False, "Should return validation failure for invalid data"