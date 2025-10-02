"""
Integration tests for Silver Layer Pipeline - TDD approach
"""

import pytest
from datetime import date

from payments_pipeline.silver.atomic_updates import AtomicSilverUpdater


class TestSilverPipelineIntegration:
    """Integration tests for the silver layer pipeline"""
    
    def test_create_silver_tables_integration(self, test_config, spark_session):
        """Test that silver tables are created successfully in real database"""
        # Arrange
        print(f"Using database: {test_config.silver_namespace}")
        updater = AtomicSilverUpdater(test_config, spark_session)
        
        # Act
        updater.create_silver_tables()
        
        # Assert - Check if database exists first
        databases = spark_session.sql("SHOW DATABASES").collect()
        db_names = [row.namespace for row in databases]
        print(f"Available databases: {db_names}")
        
        if test_config.silver_namespace in db_names:
            tables = spark_session.sql(f"SHOW TABLES IN {test_config.silver_namespace}").collect()
            table_names = [row.tableName for row in tables]
            print(f"Tables in {test_config.silver_namespace}: {table_names}")
            
            assert "dim_merchants" in table_names
            assert "fact_payments" in table_names
            assert "dim_date" in table_names
        else:
            pytest.fail(f"Database {test_config.silver_namespace} was not created")
    
    def test_should_populate_date_dimension_with_sample_data(self, test_config, spark_session):
        """Test that the silver layer can populate date dimension with business calendar logic"""
        # Arrange
        updater = AtomicSilverUpdater(test_config, spark_session)
        updater.create_silver_tables()
        
        # Act
        from datetime import date
        updater.populate_dim_date(date(2024, 1, 1), date(2024, 1, 31))
        
        # Assert
        date_count = spark_session.sql(f"SELECT COUNT(*) FROM {test_config.silver_namespace}.dim_date").collect()[0][0]
        assert date_count == 31, f"Should have 31 days, got {date_count}"
        
        # Check business calendar logic
        weekend_count = spark_session.sql(f"""
            SELECT COUNT(*) FROM {test_config.silver_namespace}.dim_date 
            WHERE is_weekend = true
        """).collect()[0][0]
        assert weekend_count > 0, "Should have weekend days"
        
        business_day_count = spark_session.sql(f"""
            SELECT COUNT(*) FROM {test_config.silver_namespace}.dim_date 
            WHERE is_business_day = true
        """).collect()[0][0]
        assert business_day_count > 0, "Should have business days"
    
    def test_should_have_merchant_processing_capability(self, test_config, spark_session):
        """Test that the silver layer has merchant processing capability"""
        # Arrange
        updater = AtomicSilverUpdater(test_config, spark_session)
        updater.create_silver_tables()
        
        # Assert
        assert hasattr(updater, 'atomic_update_merchants'), "Should have merchant processing method"
        assert callable(updater.atomic_update_merchants), "Should be callable"
        
        # Verify table structure supports SCD Type 2
        table_info = spark_session.sql(f"DESCRIBE {test_config.silver_namespace}.dim_merchants").collect()
        column_names = [row.col_name for row in table_info]
        
        assert "effective_date" in column_names, "Should have effective_date for SCD Type 2"
        assert "expiry_date" in column_names, "Should have expiry_date for SCD Type 2"
        assert "is_current" in column_names, "Should have is_current for SCD Type 2"
        assert "merchant_sk" in column_names, "Should have surrogate key"
    
    def test_should_have_payment_processing_capability(self, test_config, spark_session):
        """Test that the silver layer has payment processing capability"""
        # Arrange
        updater = AtomicSilverUpdater(test_config, spark_session)
        updater.create_silver_tables()
        
        # Assert
        assert hasattr(updater, 'atomic_update_payments'), "Should have payment processing method"
        assert callable(updater.atomic_update_payments), "Should be callable"
        
        # Verify table structure supports merchant reference
        table_info = spark_session.sql(f"DESCRIBE {test_config.silver_namespace}.fact_payments").collect()
        column_names = [row.col_name for row in table_info]
        
        assert "merchant_id" in column_names, "Should have merchant_id for direct access"
        assert "merchant_sk" in column_names, "Should have merchant_sk for dimension join"
        assert "payment_date" in column_names, "Should have payment_date for partitioning"
