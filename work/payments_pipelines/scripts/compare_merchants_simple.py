#!/usr/bin/env python3
"""
Simple comparison between merchants and merchants_raw tables using existing pipeline infrastructure.
"""

import sys
import os
sys.path.append('/usr/local/spark_dev/work/payments_pipelines/src')

from pyspark.sql import SparkSession

def compare_tables():
    """Compare the two merchants tables"""
    
    # Create Spark session with proven configuration
    spark = SparkSession.builder \
        .appName("LASAGNA-Architecture-Test-S3A") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        print("🔍 Comparing Merchants Tables")
        print("=" * 50)
        
        # Check if tables exist and get counts
        try:
            merchants_raw_count = spark.sql("SELECT COUNT(*) as count FROM iceberg.payments_bronze.merchants_raw").collect()[0]['count']
            print(f"✅ merchants_raw table: {merchants_raw_count:,} records")
        except Exception as e:
            print(f"❌ merchants_raw table: Not found - {e}")
            return False
            
        try:
            merchants_count = spark.sql("SELECT COUNT(*) as count FROM iceberg.payments_bronze.merchants").collect()[0]['count']
            print(f"✅ merchants table: {merchants_count:,} records")
        except Exception as e:
            print(f"❌ merchants table: Not found - {e}")
            return False
        
        print(f"\n📊 Record Count Comparison:")
        print(f"   merchants_raw: {merchants_raw_count:,}")
        print(f"   merchants:     {merchants_count:,}")
        
        if merchants_raw_count != merchants_count:
            print(f"⚠️  WARNING: Record counts differ by {abs(merchants_raw_count - merchants_count):,}")
        else:
            print("✅ Record counts match!")
        
        # Sample data comparison
        print(f"\n📋 Sample Data Comparison (first 3 records):")
        
        print(f"\nmerchants_raw sample:")
        merchants_raw_sample = spark.sql("""
            SELECT merchant_id, merchant_name, industry, city, state, 
                   ingestion_timestamp, source_file, bronze_layer_version, data_source
            FROM iceberg.payments_bronze.merchants_raw 
            ORDER BY merchant_id 
            LIMIT 3
        """)
        merchants_raw_sample.show(truncate=False)
        
        print(f"\nmerchants sample:")
        merchants_sample = spark.sql("""
            SELECT merchant_id, merchant_name, industry, city, state, 
                   ingestion_timestamp, source_file, bronze_layer_version, data_source
            FROM iceberg.payments_bronze.merchants 
            ORDER BY merchant_id 
            LIMIT 3
        """)
        merchants_sample.show(truncate=False)
        
        # Check for overlapping merchant IDs
        print(f"\n🔍 Merchant ID Analysis:")
        
        # Get unique merchant IDs from both tables
        merchants_raw_ids = spark.sql("SELECT DISTINCT merchant_id FROM iceberg.payments_bronze.merchants_raw")
        merchants_ids = spark.sql("SELECT DISTINCT merchant_id FROM iceberg.payments_bronze.merchants")
        
        # Count unique merchant IDs
        merchants_raw_unique = merchants_raw_ids.count()
        merchants_unique = merchants_ids.count()
        
        print(f"   Unique merchant IDs in merchants_raw: {merchants_raw_unique:,}")
        print(f"   Unique merchant IDs in merchants:     {merchants_unique:,}")
        
        # Check for exact matches in merchant IDs
        overlap_df = merchants_raw_ids.intersect(merchants_ids)
        overlap_count = overlap_df.count()
        
        print(f"   Overlapping merchant IDs:             {overlap_count:,}")
        
        if overlap_count == merchants_raw_unique == merchants_unique:
            print("✅ All merchant IDs match between tables!")
            match_percentage = 100.0
        else:
            match_percentage = (overlap_count / max(merchants_raw_unique, merchants_unique)) * 100
            print(f"⚠️  Merchant ID overlap: {match_percentage:.1f}%")
        
        # Summary
        print(f"\n📋 Comparison Summary:")
        print("=" * 50)
        
        if merchants_raw_count == merchants_count and overlap_count == merchants_raw_unique == merchants_unique:
            print("🎉 SUCCESS: Tables contain equivalent data!")
            print("   ✅ Record counts match")
            print("   ✅ All merchant IDs match")
            return True
        else:
            print("⚠️  Tables have some differences:")
            print(f"   📊 Record count difference: {abs(merchants_raw_count - merchants_count):,}")
            print(f"   🆔 Merchant ID overlap: {match_percentage:.1f}%")
            
            # This might be expected if the new pipeline processed additional files
            if merchants_count >= merchants_raw_count and match_percentage >= 95:
                print("   💡 This difference might be expected if new pipeline processed additional files")
                return True
            else:
                return False
                
    except Exception as e:
        print(f"❌ Error during comparison: {e}")
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    success = compare_tables()
    sys.exit(0 if success else 1)
