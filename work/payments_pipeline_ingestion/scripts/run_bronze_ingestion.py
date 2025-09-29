#!/usr/bin/env python3
"""
Bronze Layer Ingestion Runner

Simple script to run bronze layer ingestion from the container environment.
"""

import sys
import os
from pathlib import Path

# Add the pipeline package to Python path
sys.path.append('/usr/local/spark_dev/work/payments_pipeline_ingestion/src')

from payments_pipeline.bronze.ingestion import BronzeIngestionJob
from payments_pipeline.utils.config import PipelineConfig
from payments_pipeline.utils.spark import get_spark_session


def main():
    """Main function to run bronze ingestion"""
    
    print("🥉 Starting Bronze Layer Ingestion")
    print("=" * 50)
    
    # Configuration
    config = PipelineConfig()
    print(f"📋 Configuration: {config}")
    
    # Initialize Spark session
    print("\n🚀 Initializing Spark session...")
    spark = get_spark_session("BronzeIngestion")
    
    try:
        # Initialize bronze ingestion job
        print("\n🏗️ Initializing bronze ingestion job...")
        bronze_job = BronzeIngestionJob(config)
        
        # Set data directory
        data_dir = "/usr/local/spark_dev/work/payments_pipeline/raw_data"
        
        if not os.path.exists(data_dir):
            print(f"❌ Data directory not found: {data_dir}")
            print("   Please ensure the data generator has been run first!")
            return 1
        
        # Check available files
        print(f"\n📁 Data directory: {data_dir}")
        data_path = Path(data_dir)
        csv_files = list(data_path.glob("*.csv"))
        
        print(f"📊 Found {len(csv_files)} CSV files:")
        for file_path in csv_files:
            file_size = file_path.stat().st_size
            print(f"   {file_path.name} ({file_size:,} bytes)")
        
        # Run ingestion
        print(f"\n🚀 Starting bronze layer ingestion...")
        bronze_job.ingest_batch(data_dir)
        
        # Validate results
        print(f"\n🔍 Validating ingestion results...")
        
        # Validate merchants table
        try:
            merchants_validation = bronze_job.validate_ingestion("iceberg.payments_bronze.merchants_raw")
            print(f"✅ Merchants: {merchants_validation['row_count']:,} rows")
        except Exception as e:
            print(f"❌ Merchants validation failed: {e}")
        
        # Validate transactions table
        try:
            transactions_validation = bronze_job.validate_ingestion("iceberg.payments_bronze.transactions_raw")
            print(f"✅ Transactions: {transactions_validation['row_count']:,} rows")
        except Exception as e:
            print(f"❌ Transactions validation failed: {e}")
        
        print(f"\n🎉 Bronze layer ingestion completed successfully!")
        print(f"\n📋 Available tables:")
        spark.sql("SHOW TABLES IN iceberg.payments_bronze").show()
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Bronze ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        # Clean up Spark session
        try:
            spark.stop()
            print("\n✅ Spark session stopped")
        except:
            pass


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

