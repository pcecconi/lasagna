#!/usr/bin/env python3
"""
Bronze Layer Ingestion Runner - S3 Enabled

Complete bronze layer ingestion using S3/MinIO storage for Spark access.
"""

import sys
import os
from pathlib import Path

# Add the pipeline package to Python path
sys.path.append('/usr/local/spark_dev/work/payments_pipeline_ingestion/src')

from payments_pipeline.bronze.ingestion import BronzeIngestionJob
from payments_pipeline.utils.config import PipelineConfig
from payments_pipeline.utils.spark import get_spark_session
from payments_pipeline.utils.s3_uploader import S3Uploader
from payments_pipeline.utils.logging import setup_logging


def main():
    """Main function to run bronze ingestion with S3"""
    
    logger = setup_logging(__name__)
    logger.info("🥉 Starting Bronze Layer Ingestion with S3 Storage")
    logger.info("=" * 60)
    
    # Configuration
    config = PipelineConfig()
    logger.info(f"📋 Configuration: {config}")
    
    # Initialize Spark session
    logger.info("\n🚀 Initializing Spark session...")
    spark = get_spark_session("BronzeIngestionS3")
    
    try:
        # Initialize S3 uploader
        logger.info("\n📤 Initializing S3 uploader...")
        uploader = S3Uploader()
        
        # Set data directory
        data_dir = "/usr/local/spark_dev/work/payments_pipeline/raw_data"
        
        if not os.path.exists(data_dir):
            logger.error(f"❌ Data directory not found: {data_dir}")
            logger.error("   Please ensure the data generator has been run first!")
            return 1
        
        # Check available files
        logger.info(f"\n📁 Data directory: {data_dir}")
        data_path = Path(data_dir)
        csv_files = list(data_path.glob("*.csv"))
        
        logger.info(f"📊 Found {len(csv_files)} CSV files:")
        for file_path in csv_files:
            file_size = file_path.stat().st_size
            logger.info(f"   {file_path.name} ({file_size:,} bytes)")
        
        # Check if files are already uploaded to S3
        logger.info(f"\n📤 Checking S3 for existing files...")
        existing_files = uploader.list_uploaded_files("payments")
        
        # Check if we have the required CSV files
        merchant_files = [f for f in existing_files if "merchants" in f and f.startswith("payments/")]
        transaction_files = [f for f in existing_files if "transactions" in f and f.startswith("payments/")]
        
        if merchant_files and transaction_files:
            logger.info(f"✅ Found existing CSV files in S3, skipping upload")
            s3_paths = {
                "merchants": [f"s3a://warehouse/{f}" for f in merchant_files],
                "transactions": [f"s3a://warehouse/{f}" for f in transaction_files],
                "all_files": [f"s3a://warehouse/{f}" for f in existing_files if f.startswith("payments/")]
            }
        else:
            logger.info(f"📤 Uploading files to S3...")
            s3_paths = uploader.upload_payments_data(data_dir)
        
        # Initialize bronze ingestion job
        logger.info(f"\n🏗️ Initializing bronze ingestion job...")
        bronze_job = BronzeIngestionJob(config)
        
        # Run ingestion from S3
        logger.info(f"\n🚀 Starting bronze layer ingestion from S3...")
        
        # Ingest merchants
        if s3_paths["merchants"]:
            merchant_s3_path = s3_paths["merchants"][0]  # Use first merchant file
            logger.info(f"🏪 Ingesting merchants from {merchant_s3_path}")
            bronze_job.ingest_merchants(merchant_s3_path)
        
        # Ingest transactions
        if s3_paths["transactions"]:
            # Process initial transaction file
            initial_files = [path for path in s3_paths["transactions"] if "initial" in path]
            if initial_files:
                transaction_s3_path = initial_files[0]
                logger.info(f"💳 Ingesting initial transactions from {transaction_s3_path}")
                bronze_job.ingest_transactions(transaction_s3_path)
            
            # Process incremental transaction files
            incremental_files = [path for path in s3_paths["transactions"] if "initial" not in path]
            for s3_path in incremental_files:
                logger.info(f"🔄 Ingesting incremental transactions from {s3_path}")
                bronze_job.ingest_incremental_transactions(s3_path)
        
        # Validate results
        logger.info(f"\n🔍 Validating ingestion results...")
        
        # Validate merchants table
        merchants_validation = {"row_count": 0}
        try:
            merchants_validation = bronze_job.validate_ingestion("spark_catalog.payments_bronze.merchants_raw")
            logger.info(f"✅ Merchants: {merchants_validation['row_count']:,} rows")
        except Exception as e:
            logger.error(f"❌ Merchants validation failed: {e}")
        
        # Validate transactions table
        transactions_validation = {"row_count": 0}
        try:
            transactions_validation = bronze_job.validate_ingestion("spark_catalog.payments_bronze.transactions_raw")
            logger.info(f"✅ Transactions: {transactions_validation['row_count']:,} rows")
        except Exception as e:
            logger.error(f"❌ Transactions validation failed: {e}")
        
        # Show table information
        logger.info(f"\n📋 Bronze layer tables created:")
        spark.sql("SHOW TABLES IN spark_catalog.payments_bronze").show()
        
        # Show sample data
        logger.info(f"\n📊 Sample data:")
        try:
            logger.info("Merchants sample:")
            spark.table("spark_catalog.payments_bronze.merchants_raw").show(3)
            
            logger.info("Transactions sample:")
            spark.table("spark_catalog.payments_bronze.transactions_raw").show(3)
        except Exception as e:
            logger.error(f"❌ Error showing sample data: {e}")
        
        logger.info(f"\n🎉 Bronze layer ingestion completed successfully!")
        logger.info(f"📊 Summary:")
        logger.info(f"   Files uploaded to S3: {len(s3_paths['all_files'])}")
        logger.info(f"   Merchants ingested: {merchants_validation.get('row_count', 0):,}")
        logger.info(f"   Transactions ingested: {transactions_validation.get('row_count', 0):,}")
        
        return 0
        
    except Exception as e:
        logger.error(f"\n❌ Bronze ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        # Clean up Spark session
        try:
            spark.stop()
            logger.info("\n✅ Spark session stopped")
        except:
            pass


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

