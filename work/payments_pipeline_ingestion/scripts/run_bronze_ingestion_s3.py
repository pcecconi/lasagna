#!/usr/bin/env python3
"""
Bronze Layer Ingestion Runner - S3 Enabled

Complete bronze layer ingestion using S3/MinIO storage for Spark access.
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Add the pipeline package to Python path
sys.path.append('/usr/local/spark_dev/work/payments_pipeline_ingestion/src')

from payments_pipeline.bronze.ingestion import BronzeIngestionJob
from payments_pipeline.utils.config import PipelineConfig
from payments_pipeline.utils.spark import get_spark_session
from payments_pipeline.utils.s3_uploader import S3Uploader
from payments_pipeline.utils.logging import setup_logging

def load_processed_files_state(data_dir: Path) -> dict:
    """Load the state of processed files from state file"""
    state_file = data_dir / "ingestion_state.json"
    
    if state_file.exists():
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Failed to load state file: {e}. Starting fresh.")
    
    return {
        "processed_files": {},
        "last_updated": None
    }

def save_processed_files_state(data_dir: Path, state: dict):
    """Save the state of processed files to state file"""
    state_file = data_dir / "ingestion_state.json"
    
    try:
        state["last_updated"] = datetime.now().isoformat()
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        print(f"💾 Saved ingestion state to {state_file}")
    except IOError as e:
        print(f"Error: Failed to save state file: {e}")

def get_file_hash(file_path: Path) -> str:
    """Get a simple hash of file for change detection"""
    try:
        stat = file_path.stat()
        # Use file size and modification time as a simple change indicator
        return f"{stat.st_size}_{stat.st_mtime}"
    except OSError:
        return "unknown"

def get_new_files(csv_files: list, processed_state: dict) -> list:
    """Identify which files are new or have changed"""
    new_files = []
    
    for file_path in csv_files:
        filename = file_path.name
        current_hash = get_file_hash(file_path)
        
        # Check if file is new or has changed
        if filename not in processed_state["processed_files"]:
            new_files.append(file_path)
            print(f"📄 New file detected: {filename}")
        elif processed_state["processed_files"][filename]["hash"] != current_hash:
            new_files.append(file_path)
            print(f"📄 Changed file detected: {filename}")
        else:
            print(f"⏭️ Skipping already processed file: {filename}")
    
    return new_files

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
        
        # Check available files and load processing state
        logger.info(f"\n📁 Data directory: {data_dir}")
        data_path = Path(data_dir)
        csv_files = list(data_path.glob("*.csv"))
        
        # Load state of previously processed files
        processed_state = load_processed_files_state(data_path)
        logger.info(f"📋 Loaded processing state: {len(processed_state['processed_files'])} files previously processed")
        
        # Identify new or changed files
        new_files = get_new_files(csv_files, processed_state)
        
        logger.info(f"📊 Found {len(csv_files)} total CSV files, {len(new_files)} new/changed:")
        for file_path in csv_files:
            file_size = file_path.stat().st_size
            status = "🆕 NEW" if file_path in new_files else "✅ PROCESSED"
            logger.info(f"   {file_path.name} ({file_size:,} bytes) - {status}")
        
        if not new_files:
            logger.info("🎉 No new files to process. Exiting.")
            return
        
        # Clean up old S3 files first to avoid confusion
        logger.info(f"\n🧹 Cleaning up old S3 files...")
        uploader.cleanup_old_files("payments", keep_days=0)  # Remove all old files
        uploader.cleanup_database_files("payments_bronze")  # Remove orphaned database metadata
        
        # Upload only new/changed files to S3
        logger.info(f"\n📤 Uploading {len(new_files)} new/changed files to S3...")
        s3_paths = uploader.upload_payments_data(data_dir, new_files)
        
        # Initialize bronze ingestion job
        logger.info(f"\n🏗️ Initializing bronze ingestion job...")
        bronze_job = BronzeIngestionJob(config)
        
        # Ensure database and tables exist (safe for incremental loads)
        logger.info(f"\n🏗️ Ensuring database and tables exist...")
        bronze_job.create_database(config.bronze_namespace)
        bronze_job.create_merchants_table(config.bronze_namespace)
        bronze_job.create_transactions_table(config.bronze_namespace)
        
        # Run ingestion from S3
        logger.info(f"\n🚀 Starting bronze layer ingestion from S3...")
        
        # Track which files we successfully process
        successfully_processed = []
        
        # Ingest merchants
        if s3_paths["merchants"]:
            merchant_s3_path = s3_paths["merchants"][0]  # Use first merchant file
            logger.info(f"🏪 Ingesting merchants from {merchant_s3_path}")
            try:
                bronze_job.ingest_merchants(merchant_s3_path)
                # Find the corresponding local file
                merchant_filename = merchant_s3_path.split('/')[-1]
                merchant_local_file = data_path / merchant_filename
                if merchant_local_file.exists():
                    successfully_processed.append(merchant_local_file)
            except Exception as e:
                logger.error(f"❌ Failed to ingest merchants: {e}")
        
        # Ingest transactions
        if s3_paths["transactions"]:
            # Process all transaction files in chronological order
            for s3_path in s3_paths["transactions"]:
                # Determine if this is an initial or incremental file
                filename = s3_path.split('/')[-1]  # Extract filename from S3 path
                
                try:
                    if bronze_job._is_initial_file(filename):
                        logger.info(f"💳 Ingesting initial transactions from {s3_path}")
                        bronze_job.ingest_transactions(s3_path)
                    else:
                        logger.info(f"🔄 Ingesting incremental transactions from {s3_path}")
                        bronze_job.ingest_incremental_transactions(s3_path)
                    
                    # Find the corresponding local file
                    transaction_local_file = data_path / filename
                    if transaction_local_file.exists():
                        successfully_processed.append(transaction_local_file)
                        
                except Exception as e:
                    logger.error(f"❌ Failed to ingest transactions from {s3_path}: {e}")
        
        # Validate results
        logger.info(f"\n🔍 Validating ingestion results...")
        
        # Validate merchants table
        merchants_validation = {"row_count": 0}
        try:
            merchants_validation = bronze_job.validate_ingestion(f"{config.iceberg_catalog}.{config.bronze_namespace}.merchants_raw")
            logger.info(f"✅ Merchants: {merchants_validation['row_count']:,} rows")
        except Exception as e:
            logger.error(f"❌ Merchants validation failed: {e}")
        
        # Validate transactions table
        transactions_validation = {"row_count": 0}
        try:
            transactions_validation = bronze_job.validate_ingestion(f"{config.iceberg_catalog}.{config.bronze_namespace}.transactions_raw")
            logger.info(f"✅ Transactions: {transactions_validation['row_count']:,} rows")
        except Exception as e:
            logger.error(f"❌ Transactions validation failed: {e}")
        
        # Show table information
        logger.info(f"\n📋 Bronze layer tables created:")
        spark.sql(f"SHOW TABLES IN {config.iceberg_catalog}.{config.bronze_namespace}").show()
        
        # Show sample data
        logger.info(f"\n📊 Sample data:")
        try:
            logger.info("Merchants sample:")
            spark.table(f"{config.iceberg_catalog}.{config.bronze_namespace}.merchants_raw").show(3)
            
            logger.info("Transactions sample:")
            spark.table(f"{config.iceberg_catalog}.{config.bronze_namespace}.transactions_raw").show(3)
        except Exception as e:
            logger.error(f"❌ Error showing sample data: {e}")
        
        # Run comprehensive data quality validation
        logger.info(f"\n🔍 Running comprehensive data quality validation...")
        try:
            from payments_pipeline.bronze.validation import DataQualityValidator
            
            validator = DataQualityValidator(spark)
            
            # Validate merchants table
            logger.info("Validating merchants_raw table...")
            merchants_dq_results = validator.run_comprehensive_validation(f"{config.iceberg_catalog}.{config.bronze_namespace}.merchants_raw")
            
            # Validate transactions table
            logger.info("Validating transactions_raw table...")
            transactions_dq_results = validator.run_comprehensive_validation(f"{config.iceberg_catalog}.{config.bronze_namespace}.transactions_raw")
            
            # Check validation results
            merchants_passed = merchants_dq_results.get("data_quality", {}).get("overall_passed", False)
            transactions_passed = transactions_dq_results.get("data_quality", {}).get("overall_passed", False)
            
            if merchants_passed and transactions_passed:
                logger.info("✅ All data quality validations passed!")
            else:
                logger.warning("⚠️ Some data quality validations failed:")
                if not merchants_passed:
                    logger.warning("   - Merchants validation failed")
                if not transactions_passed:
                    logger.warning("   - Transactions validation failed")
                    
        except Exception as e:
            logger.error(f"❌ Data quality validation failed: {e}")
            logger.warning("⚠️ Proceeding with cleanup despite validation errors...")
        
        # Update state file with successfully processed files
        if successfully_processed:
            logger.info(f"\n💾 Updating processing state for {len(successfully_processed)} files...")
            for file_path in successfully_processed:
                filename = file_path.name
                file_hash = get_file_hash(file_path)
                processed_state["processed_files"][filename] = {
                    "hash": file_hash,
                    "processed_at": datetime.now().isoformat(),
                    "file_size": file_path.stat().st_size
                }
            
            save_processed_files_state(data_path, processed_state)
        
        logger.info(f"\n🎉 Bronze layer ingestion completed successfully!")
        logger.info(f"📊 Summary:")
        logger.info(f"   Files uploaded to S3: {len(s3_paths['all_files'])}")
        logger.info(f"   Merchants ingested: {merchants_validation.get('row_count', 0):,}")
        logger.info(f"   Transactions ingested: {transactions_validation.get('row_count', 0):,}")
        
        # Clean up S3 files after successful ingestion and validation
        logger.info(f"\n🧹 Cleaning up S3 files after ingestion...")
        uploader.cleanup_old_files("payments", keep_days=0)  # Remove all files
        uploader.cleanup_database_files("payments_bronze")  # Remove database metadata
        
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

