#!/usr/bin/env python3
"""
Setup Payments Databases Script

Recreates all payments databases from scratch with sample data.
This script provides a clean, automated way to set up the entire payments pipeline.

Usage:
    python scripts/setup_payments_databases.py [--with-sample-data] [--bronze-only] [--silver-only]
    
Options:
    --with-sample-data    Ingest sample data from the data generator
    --bronze-only         Only create bronze layer
    --silver-only         Only create silver layer (requires bronze to exist)
"""

import argparse
import sys
from pathlib import Path
from datetime import date

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from payments_pipeline.bronze.ingestion import BronzeIngestionJob
from payments_pipeline.silver.silver_ingestion import SilverIngestionJob
from payments_pipeline.utils.spark import get_spark_session
from payments_pipeline.utils.config import PipelineConfig
from payments_pipeline.utils.s3_uploader import S3Uploader


def setup_bronze_layer(config, spark, with_sample_data=False):
    """Set up the bronze layer with optional sample data"""
    print("📦 Setting up Bronze Layer...")
    
    bronze_job = BronzeIngestionJob(config, spark)
    
    # Create bronze database (tables will be created during ingestion)
    bronze_job.create_database()
    print("✅ Bronze database created successfully")
    
    if with_sample_data:
        print("📊 Ingesting sample data into Bronze Layer...")
        
        # Check if sample data files exist
        sample_data_dir = Path("/usr/local/spark_dev/work/payments_data_source/raw_data")
        
        if sample_data_dir.exists():
            # Initialize S3 uploader
            uploader = S3Uploader()
            
            # Check if files are already uploaded to S3
            existing_files = uploader.list_uploaded_files("payments")
            merchant_files = [f for f in existing_files if "merchants" in f and f.startswith("payments/")]
            transaction_files = [f for f in existing_files if "transactions" in f and f.startswith("payments/")]
            
            if merchant_files and transaction_files:
                print("✅ Found existing CSV files in S3, using them for ingestion")
                s3_paths = {
                    "merchants": [f"s3a://warehouse/{f}" for f in merchant_files],
                    "transactions": [f"s3a://warehouse/{f}" for f in transaction_files]
                }
            else:
                print("📤 Uploading files to S3...")
                s3_paths = uploader.upload_payments_data(str(sample_data_dir))
            
            # Ingest merchants
            if s3_paths["merchants"]:
                merchant_s3_path = s3_paths["merchants"][0]
                print(f"🏪 Ingesting merchants from {merchant_s3_path}")
                bronze_job.ingest_merchants(merchant_s3_path)
            
            # Ingest transactions
            if s3_paths["transactions"]:
                initial_files = [path for path in s3_paths["transactions"] if "initial" in path]
                if initial_files:
                    transaction_s3_path = initial_files[0]
                    print(f"💳 Ingesting initial transactions from {transaction_s3_path}")
                    bronze_job.ingest_transactions(transaction_s3_path)
            
            print("✅ Sample data ingested into bronze layer")
        else:
            print("⚠️  Sample data directory not found, skipping data ingestion")
            print(f"   Expected directory: {sample_data_dir}")
    
    return bronze_job


def setup_silver_layer(config, spark, bronze_job=None):
    """Set up the silver layer"""
    print("🎯 Setting up Silver Layer...")
    
    silver_job = SilverIngestionJob(config, spark)
    
    # Create silver tables
    silver_job.atomic_updater.create_silver_tables()
    print("✅ Silver tables created successfully")
    
    # Populate date dimension
    print("📅 Populating date dimension...")
    silver_job.atomic_updater.populate_dim_date(date(2024, 1, 1), date(2024, 12, 31))
    print("✅ Date dimension populated")
    
    # Process data from bronze to silver if bronze data exists
    if bronze_job:
        print("🔄 Processing data from Bronze to Silver...")
        
        try:
            # Get bronze data
            bronze_merchants_df = spark.table(f'{config.iceberg_catalog}.{config.bronze_namespace}.merchants_raw')
            bronze_payments_df = spark.table(f'{config.iceberg_catalog}.{config.bronze_namespace}.transactions_raw')
            
            # Process merchants
            print("  Processing merchants...")
            merchant_result = silver_job.atomic_updater.atomic_update_merchants(bronze_merchants_df)
            print(f"  ✅ Merchant processing: {'SUCCESS' if merchant_result else 'FAILED'}")
            
            # Process payments
            print("  Processing payments...")
            payment_result = silver_job.atomic_updater.atomic_update_payments(
                bronze_payments_df, (date(2024, 1, 1), date(2024, 1, 31))
            )
            print(f"  ✅ Payment processing: {'SUCCESS' if payment_result else 'FAILED'}")
            
        except Exception as e:
            print(f"  ⚠️  Could not process bronze data: {e}")
            print("  Continuing with empty silver layer...")
    
    return silver_job


def run_data_quality_checks(silver_job):
    """Run data quality checks on the silver layer"""
    print("🔍 Running data quality checks...")
    
    try:
        dq_results = silver_job.data_quality_checker.run_all_checks()
        print("✅ Data quality checks completed")
        
        # Print summary
        if 'summary' in dq_results:
            summary = dq_results['summary']
            print(f"   Total checks: {summary.get('total_checks', 'N/A')}")
            print(f"   Failed checks: {summary.get('failed_checks', 'N/A')}")
            print(f"   Status: {'PASS' if summary.get('failed_checks', 0) == 0 else 'FAIL'}")
        
    except Exception as e:
        print(f"⚠️  Data quality checks failed: {e}")


def get_final_statistics(silver_job):
    """Get final statistics from the silver layer"""
    print("📈 Getting final statistics...")
    
    try:
        stats = silver_job.get_silver_layer_stats()
        print("✅ Statistics retrieved")
        
        # Print key statistics
        if stats:
            print("   Key Statistics:")
            for key, value in stats.items():
                print(f"     {key}: {value}")
        
    except Exception as e:
        print(f"⚠️  Could not retrieve statistics: {e}")


def main():
    """Main setup function"""
    parser = argparse.ArgumentParser(description='Setup Payments Databases')
    parser.add_argument('--with-sample-data', action='store_true', 
                       help='Ingest sample data from the data generator')
    parser.add_argument('--bronze-only', action='store_true', 
                       help='Only create bronze layer')
    parser.add_argument('--silver-only', action='store_true', 
                       help='Only create silver layer (requires bronze to exist)')
    
    args = parser.parse_args()
    
    print("🚀 Setting up Payments Databases...")
    print("=" * 60)
    
    # Initialize components
    config = PipelineConfig()
    spark = get_spark_session()
    
    bronze_job = None
    silver_job = None
    
    try:
        # Set up bronze layer
        if not args.silver_only:
            bronze_job = setup_bronze_layer(config, spark, args.with_sample_data)
        
        # Set up silver layer
        if not args.bronze_only:
            silver_job = setup_silver_layer(config, spark, bronze_job)
            
            # Run data quality checks
            run_data_quality_checks(silver_job)
            
            # Get final statistics
            get_final_statistics(silver_job)
        
        print("\n" + "=" * 60)
        print("🎉 Payments databases setup completed successfully!")
        print("=" * 60)
        
        # Print summary
        print("\n📋 Setup Summary:")
        if bronze_job:
            print("  ✅ Bronze layer created")
        if silver_job:
            print("  ✅ Silver layer created")
        
        print("\n💡 Next steps:")
        print("  - Run data quality checks: python scripts/run_data_quality.py")
        print("  - Generate more sample data: python scripts/generate_sample_data.py")
        print("  - View data in notebooks: jupyter lab")
        
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
