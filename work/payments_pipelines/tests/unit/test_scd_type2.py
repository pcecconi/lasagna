#!/usr/bin/env python3
"""
Unit tests for SCD Type 2 processing logic
"""

import pytest
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType, BooleanType

from src.payments_pipeline.silver.atomic_updates import AtomicSilverUpdater
from src.payments_pipeline.utils.config import PipelineConfig


class TestSCDType2Processing:
    """Test SCD Type 2 processing logic"""
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for testing"""
        return SparkSession.builder \
            .appName("SCD Type 2 Tests") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
    
    @pytest.fixture
    def atomic_updater(self, spark_session):
        """Create AtomicSilverUpdater instance"""
        config = PipelineConfig()
        return AtomicSilverUpdater(config, spark_session)
    
    @pytest.fixture
    def sample_merchant_data(self, spark_session):
        """Create sample merchant data with multiple versions"""
        schema = StructType([
            StructField("merchant_id", StringType(), False),
            StructField("merchant_name", StringType(), False),
            StructField("industry", StringType(), False),
            StructField("address", StringType(), False),
            StructField("city", StringType(), False),
            StructField("state", StringType(), False),
            StructField("zip_code", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("email", StringType(), False),
            StructField("mdr_rate", DoubleType(), False),
            StructField("size_category", StringType(), False),
            StructField("creation_date", DateType(), False),
            StructField("effective_date", DateType(), False),
            StructField("version", IntegerType(), False),
            StructField("change_type", StringType(), False),
        ])
        
        # Sample data with merchant having multiple versions
        data = [
            # Merchant M000001 - Version 1 (initial)
            ("M000001", "Test Store 1", "retail", "123 Main St", "New York", "NY", "10001", 
             "555-123-4567", "contact@m000001.com", 0.0338, "small", 
             date(2024, 1, 1), date(2024, 1, 1), 1, "initial"),
            
            # Merchant M000001 - Version 2 (attribute change)
            ("M000001", "Test Store 1", "retail", "123 Main St", "New York", "NY", "10001", 
             "555-123-4567", "contact@m000001.com", 0.0340, "small", 
             date(2024, 1, 1), date(2024, 1, 3), 2, "attribute_change"),
            
            # Merchant M000001 - Version 3 (attribute change)
            ("M000001", "Test Store 1", "retail", "123 Main St", "Los Angeles", "CA", "10001", 
             "555-123-4567", "contact@m000001.com", 0.0340, "small", 
             date(2024, 1, 1), date(2024, 1, 9), 3, "attribute_change"),
            
            # Merchant M000001 - Version 4 (attribute change) - Current version
            ("M000001", "Test Store 1", "retail", "123 Main St", "Los Angeles", "CA", "90210", 
             "555-123-4567", "contact@m000001.com", 0.0326, "small", 
             date(2024, 1, 1), date(2024, 1, 15), 4, "attribute_change"),
            
            # Merchant M000002 - Version 1 (initial)
            ("M000002", "Test Store 2", "fitness", "456 Oak St", "Chicago", "IL", "60601", 
             "555-987-6543", "contact@m000002.com", 0.0311, "medium", 
             date(2024, 1, 1), date(2024, 1, 1), 1, "initial"),
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    def test_scd_type2_processing(self, atomic_updater, sample_merchant_data):
        """Test SCD Type 2 processing with sample data"""
        # Process the data
        result_df = atomic_updater._process_scd_type2_merchants(sample_merchant_data)
        
        # Collect results for verification
        results = result_df.collect()
        
        # Group by merchant_id for easier testing
        merchants = {}
        for row in results:
            merchant_id = row.merchant_id
            if merchant_id not in merchants:
                merchants[merchant_id] = []
            merchants[merchant_id].append(row)
        
        # Test merchant M000001 (has multiple versions)
        m000001_versions = merchants["M000001"]
        assert len(m000001_versions) == 4, f"Expected 4 versions for M000001, got {len(m000001_versions)}"
        
        # Sort by version for testing
        m000001_versions.sort(key=lambda x: x.version)
        
        # Test Version 1
        v1 = m000001_versions[0]
        assert v1.version == 1
        assert v1.effective_date == date(2024, 1, 1)
        assert v1.expiry_date == date(2024, 1, 2)  # Day before version 2's effective_date
        assert v1.is_current == False
        
        # Test Version 2
        v2 = m000001_versions[1]
        assert v2.version == 2
        assert v2.effective_date == date(2024, 1, 3)
        assert v2.expiry_date == date(2024, 1, 8)  # Day before version 3's effective_date
        assert v2.is_current == False
        
        # Test Version 3
        v3 = m000001_versions[2]
        assert v3.version == 3
        assert v3.effective_date == date(2024, 1, 9)
        assert v3.expiry_date == date(2024, 1, 14)  # Day before version 4's effective_date
        assert v3.is_current == False
        
        # Test Version 4 (current)
        v4 = m000001_versions[3]
        assert v4.version == 4
        assert v4.effective_date == date(2024, 1, 15)
        assert v4.expiry_date == date(9999, 12, 31)  # End of time for current version
        assert v4.is_current == True
        
        # Test merchant M000002 (single version)
        m000002_versions = merchants["M000002"]
        assert len(m000002_versions) == 1, f"Expected 1 version for M000002, got {len(m000002_versions)}"
        
        v1_m000002 = m000002_versions[0]
        assert v1_m000002.version == 1
        assert v1_m000002.effective_date == date(2024, 1, 1)
        assert v1_m000002.expiry_date == date(9999, 12, 31)  # End of time for current version
        assert v1_m000002.is_current == True
    
    def test_scd_type2_no_overlapping_dates(self, atomic_updater, sample_merchant_data):
        """Test that SCD Type 2 processing produces non-overlapping date ranges"""
        result_df = atomic_updater._process_scd_type2_merchants(sample_merchant_data)
        
        # Check for overlapping date ranges
        overlapping_check = atomic_updater.spark.sql(f"""
            SELECT DISTINCT a.merchant_id
            FROM ({result_df.createOrReplaceTempView('temp_result'); 
                  'temp_result'}) a
            JOIN ({result_df.createOrReplaceTempView('temp_result2'); 
                   'temp_result2'}) b
            ON a.merchant_id = b.merchant_id 
            AND a.merchant_sk != b.merchant_sk
            AND a.is_current = false 
            AND b.is_current = false
            WHERE (a.effective_date <= b.effective_date AND a.expiry_date > b.effective_date)
               OR (b.effective_date <= a.effective_date AND b.expiry_date > a.effective_date)
        """)
        
        overlapping_count = overlapping_check.count()
        assert overlapping_count == 0, f"Found {overlapping_count} overlapping date ranges in SCD Type 2 processing"
    
    def test_scd_type2_exactly_one_current_per_merchant(self, atomic_updater, sample_merchant_data):
        """Test that each merchant has exactly one current record"""
        result_df = atomic_updater._process_scd_type2_merchants(sample_merchant_data)
        
        # Check current record counts
        current_check = atomic_updater.spark.sql(f"""
            SELECT 
                merchant_id,
                COUNT(*) as version_count,
                SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) as current_count
            FROM ({result_df.createOrReplaceTempView('temp_current_check'); 'temp_current_check'})
            GROUP BY merchant_id
            HAVING SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) != 1
        """)
        
        issues = current_check.count()
        assert issues == 0, f"Found {issues} merchants with incorrect current record counts"


if __name__ == "__main__":
    pytest.main([__file__])
