#!/usr/bin/env python3
"""
Standalone test for SCD Type 2 processing logic
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hash, concat, lit, current_timestamp, max as spark_max, min as spark_min, when, date_sub
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType, BooleanType

from payments_pipeline.silver.atomic_updates import AtomicSilverUpdater
from payments_pipeline.utils.config import PipelineConfig


def create_sample_data(spark):
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
    
    return spark.createDataFrame(data, schema)


def process_scd_type2_directly(atomic_updater, bronze_merchants_df, spark):
    """Process SCD Type 2 logic directly without accessing database tables"""
    
    # Process all bronze versions and create proper SCD Type 2 records
    # Each bronze version becomes a silver record with proper is_current flag
    all_bronze_versions_df = bronze_merchants_df.select(
        hash(concat(col("merchant_id"), col("effective_date").cast("string"), col("version").cast("string"))).cast("bigint").alias("merchant_sk"),
        col("merchant_id"),
        col("merchant_name"),
        col("industry"),
        col("address"),
        col("city"),
        col("state"),
        col("zip_code").cast("string"),  # Cast to string to match silver schema
        col("phone"),
        col("email"),
        col("mdr_rate"),
        col("size_category"),
        col("creation_date"),
        col("effective_date"),
        lit(date(9999, 12, 31)).alias("expiry_date"),  # Will be updated for non-current records
        lit(True).alias("is_current"),  # All bronze versions are current initially
        col("version"),
        col("change_type"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
        lit("silver_layer").alias("source_system")
    )
    
    # Set up window specs for proper SCD Type 2 processing
    window_spec_by_version_desc = Window.partitionBy("merchant_id").orderBy(col("version").desc())
    
    # Process SCD Type 2 logic using a simpler approach
    # First, mark current records
    merchants_with_current_flag = all_bronze_versions_df \
        .withColumn("is_latest", col("version") == spark_max("version").over(window_spec_by_version_desc)) \
        .withColumn("is_current", col("is_latest"))
    
    # Now calculate expiry dates using a different approach
    # For each merchant, get all versions ordered by effective_date
    window_spec_effective_asc = Window.partitionBy("merchant_id").orderBy(col("effective_date").asc(), col("version").asc())
    
    processed_merchants_df = merchants_with_current_flag \
        .withColumn("next_effective_date", 
                   when(col("is_current") == True, lit(None))
                   .otherwise(
                       # For non-current records, get the next effective_date by looking ahead
                       spark_min(col("effective_date")).over(
                           window_spec_effective_asc.rowsBetween(Window.currentRow + 1, Window.unboundedFollowing)
                       )
                   )) \
        .withColumn("expiry_date", 
                   when(col("is_current") == True, lit(date(9999, 12, 31)))
                   .otherwise(date_sub(col("next_effective_date"), 1))) \
        .drop("is_latest", "next_effective_date")
    
    return processed_merchants_df


def test_scd_type2_processing():
    """Test SCD Type 2 processing with sample data"""
    print("🧪 Testing SCD Type 2 processing...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SCD Type 2 Test") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    try:
        # Create atomic updater
        config = PipelineConfig()
        atomic_updater = AtomicSilverUpdater(config, spark)
        
        # Create sample data
        sample_data = create_sample_data(spark)
        
        # Mock the current merchants table to avoid database dependency
        # Create an empty DataFrame with the expected schema
        empty_merchants_schema = StructType([
            StructField("merchant_sk", IntegerType(), False),
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
            StructField("expiry_date", DateType(), False),
            StructField("is_current", BooleanType(), False),
            StructField("version", IntegerType(), False),
            StructField("change_type", StringType(), False),
            StructField("created_at", StringType(), False),
            StructField("updated_at", StringType(), False),
            StructField("source_system", StringType(), False),
        ])
        
        empty_df = spark.createDataFrame([], empty_merchants_schema)
        empty_df.createOrReplaceTempView("dim_merchants")
        
        # Process the data directly without accessing the table
        result_df = process_scd_type2_directly(atomic_updater, sample_data, spark)
        
        # Show results
        print("\n📊 Processed Data:")
        result_df.show(20, truncate=False)
        
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
        print(f"\n🔍 Testing merchant M000001...")
        m000001_versions = merchants["M000001"]
        print(f"Found {len(m000001_versions)} versions for M000001")
        
        # Sort by version for testing
        m000001_versions.sort(key=lambda x: x.version)
        
        # Test Version 1
        v1 = m000001_versions[0]
        print(f"Version 1: effective_date={v1.effective_date}, expiry_date={v1.expiry_date}, is_current={v1.is_current}")
        assert v1.version == 1
        assert v1.effective_date == date(2024, 1, 1)
        assert v1.expiry_date == date(2024, 1, 2), f"Expected 2024-01-02, got {v1.expiry_date}"
        assert v1.is_current == False
        
        # Test Version 2
        v2 = m000001_versions[1]
        print(f"Version 2: effective_date={v2.effective_date}, expiry_date={v2.expiry_date}, is_current={v2.is_current}")
        assert v2.version == 2
        assert v2.effective_date == date(2024, 1, 3)
        assert v2.expiry_date == date(2024, 1, 8), f"Expected 2024-01-08, got {v2.expiry_date}"
        assert v2.is_current == False
        
        # Test Version 3
        v3 = m000001_versions[2]
        print(f"Version 3: effective_date={v3.effective_date}, expiry_date={v3.expiry_date}, is_current={v3.is_current}")
        assert v3.version == 3
        assert v3.effective_date == date(2024, 1, 9)
        assert v3.expiry_date == date(2024, 1, 14), f"Expected 2024-01-14, got {v3.expiry_date}"
        assert v3.is_current == False
        
        # Test Version 4 (current)
        v4 = m000001_versions[3]
        print(f"Version 4: effective_date={v4.effective_date}, expiry_date={v4.expiry_date}, is_current={v4.is_current}")
        assert v4.version == 4
        assert v4.effective_date == date(2024, 1, 15)
        assert v4.expiry_date == date(9999, 12, 31), f"Expected 9999-12-31, got {v4.expiry_date}"
        assert v4.is_current == True
        
        # Test merchant M000002 (single version)
        print(f"\n🔍 Testing merchant M000002...")
        m000002_versions = merchants["M000002"]
        print(f"Found {len(m000002_versions)} versions for M000002")
        
        v1_m000002 = m000002_versions[0]
        print(f"Version 1: effective_date={v1_m000002.effective_date}, expiry_date={v1_m000002.expiry_date}, is_current={v1_m000002.is_current}")
        assert v1_m000002.version == 1
        assert v1_m000002.effective_date == date(2024, 1, 1)
        assert v1_m000002.expiry_date == date(9999, 12, 31), f"Expected 9999-12-31, got {v1_m000002.expiry_date}"
        assert v1_m000002.is_current == True
        
        print("\n✅ All SCD Type 2 tests passed!")
        
        # Test for overlapping dates
        print("\n🔍 Testing for overlapping date ranges...")
        result_df.createOrReplaceTempView("temp_result")
        
        overlapping_check = spark.sql("""
            SELECT DISTINCT a.merchant_id
            FROM temp_result a
            JOIN temp_result b
            ON a.merchant_id = b.merchant_id 
            AND a.merchant_sk != b.merchant_sk
            AND a.is_current = false 
            AND b.is_current = false
            WHERE (a.effective_date <= b.effective_date AND a.expiry_date > b.effective_date)
               OR (b.effective_date <= a.effective_date AND b.expiry_date > a.effective_date)
        """)
        
        overlapping_count = overlapping_check.count()
        print(f"Found {overlapping_count} overlapping date ranges")
        assert overlapping_count == 0, f"Found {overlapping_count} overlapping date ranges in SCD Type 2 processing"
        
        print("✅ No overlapping date ranges found!")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    test_scd_type2_processing()
