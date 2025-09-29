#!/usr/bin/env python3
"""
Bronze Layer Ingestion

ELT approach: Raw data ingestion with minimal transformation.
Preserves original data structure for maximum flexibility.
Combines CSV ingestion capabilities with Iceberg table management.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, input_file_name, 
    regexp_extract, when, isnan, isnull
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from ..utils.spark import get_spark_session
from ..utils.config import PipelineConfig
from ..utils.logging import setup_logging


class BronzeIngestionJob:
    """
    Bronze Layer Ingestion Job
    
    Performs raw data ingestion with minimal transformation:
    - Preserves original CSV structure
    - Adds metadata columns (ingestion_timestamp, source_file)
    - Basic data quality checks
    - Ingests into Iceberg tables
    - Supports table creation and management
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None, spark_session=None):
        self.config = config or PipelineConfig()
        self.logger = setup_logging(__name__)
        
        # Use provided Spark session or get from utils
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = get_spark_session()
        
        # Set up Iceberg catalog
        self._setup_iceberg_catalog()
    
    def _setup_iceberg_catalog(self):
        """Set up Iceberg catalog configuration"""
        try:
            # Create the namespace first
            self.spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.payments_bronze")
            self.logger.info("✅ Iceberg catalog configured")
        except Exception as e:
            self.logger.warning(f"⚠️ Catalog setup warning: {e}")
            # Continue anyway as the catalog might already exist
    
    def create_database(self, database_name="payments_bronze"):
        """Create database if it doesn't exist"""
        self.logger.info(f"Creating database: {database_name}")
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS spark_catalog.{database_name}")
        self.logger.info(f"✅ Database {database_name} created successfully")
    
    def create_merchants_table(self, database_name="payments_bronze"):
        """Create merchants_raw Iceberg table with proper schema"""
        self.logger.info("Creating merchants_raw table...")
        
        create_table_sql = f"""
        CREATE OR REPLACE TABLE spark_catalog.{database_name}.merchants_raw (
            merchant_id STRING,
            merchant_name STRING,
            industry STRING,
            address STRING,
            city STRING,
            state STRING,
            zip_code INT,
            phone STRING,
            email STRING,
            mdr_rate DOUBLE,
            size_category STRING,
            creation_date DATE,
            status STRING,
            last_transaction_date STRING,
            ingestion_timestamp TIMESTAMP,
            source_file STRING,
            bronze_layer_version STRING,
            data_source STRING
        ) USING iceberg
        LOCATION 's3a://warehouse/{database_name}.db/merchants_raw'
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'zstd',
            'write.metadata.compression-codec' = 'gzip'
        )
        """
        
        self.spark.sql(create_table_sql)
        self.logger.info("✅ merchants_raw table created successfully")
    
    def create_transactions_table(self, database_name="payments_bronze"):
        """Create transactions_raw Iceberg table with proper schema"""
        self.logger.info("Creating transactions_raw table...")
        
        create_table_sql = f"""
        CREATE OR REPLACE TABLE spark_catalog.{database_name}.transactions_raw (
            payment_id STRING,
            payment_timestamp TIMESTAMP,
            payment_lat DOUBLE,
            payment_lng DOUBLE,
            payment_amount DOUBLE,
            payment_type STRING,
            terminal_id STRING,
            card_type STRING,
            card_issuer STRING,
            card_brand STRING,
            payment_status STRING,
            merchant_id STRING,
            transactional_cost_rate DOUBLE,
            transactional_cost_amount DOUBLE,
            mdr_amount DOUBLE,
            net_profit DOUBLE,
            ingestion_timestamp TIMESTAMP,
            source_file STRING,
            bronze_layer_version STRING,
            data_source STRING
        ) USING iceberg
        LOCATION 's3a://warehouse/{database_name}.db/transactions_raw'
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'zstd',
            'write.metadata.compression-codec' = 'gzip'
        )
        """
        
        self.spark.sql(create_table_sql)
        self.logger.info("✅ transactions_raw table created successfully")
    
    
    def verify_tables(self, database_name="payments_bronze"):
        """Verify that tables were created correctly and are accessible"""
        self.logger.info("Verifying tables...")
        
        # Check table counts
        merchants_count = self.spark.sql(f"SELECT COUNT(*) FROM spark_catalog.{database_name}.merchants_raw").collect()[0][0]
        transactions_count = self.spark.sql(f"SELECT COUNT(*) FROM spark_catalog.{database_name}.transactions_raw").collect()[0][0]
        
        self.logger.info(f"✅ merchants_raw: {merchants_count} records")
        self.logger.info(f"✅ transactions_raw: {transactions_count} records")
        
        return True
    
    def drop_database(self, database_name="payments_bronze"):
        """Drop database and all tables"""
        self.logger.info(f"Dropping database: {database_name}")
        # First drop tables, then database
        try:
            self.spark.sql(f"DROP TABLE IF EXISTS spark_catalog.{database_name}.merchants_raw")
            self.spark.sql(f"DROP TABLE IF EXISTS spark_catalog.{database_name}.transactions_raw")
        except:
            pass  # Tables might not exist
        self.spark.sql(f"DROP NAMESPACE IF EXISTS spark_catalog.{database_name}")
        self.logger.info(f"✅ Database {database_name} dropped successfully")
    
    def recreate_database(self, database_name="payments_bronze"):
        """Drop and recreate database with proper Iceberg tables"""
        self.logger.info(f"Recreating database: {database_name}")
        
        # Drop existing database (skip if it doesn't exist)
        try:
            self.drop_database(database_name)
        except:
            self.logger.info(f"Database {database_name} doesn't exist, skipping drop")
        
        # Create new database and tables
        self.create_database(database_name)
        self.create_merchants_table(database_name)
        self.create_transactions_table(database_name)
        self.verify_tables(database_name)
        
        self.logger.info(f"✅ Database {database_name} recreated successfully")
    
    def ingest_merchants(self, source_path: str, target_table: str = "spark_catalog.payments_bronze.merchants_raw"):
        """
        Ingest raw merchant data
        
        Args:
            source_path: Path to merchant CSV file
            target_table: Target Iceberg table name
        """
        self.logger.info(f"🏪 Ingesting merchants from {source_path}")
        
        # Ensure database exists
        self.create_database("payments_bronze")
        
        # Create table if it doesn't exist
        self.create_merchants_table("payments_bronze")
        
        # Read CSV with schema inference
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        # Add bronze layer metadata
        df_bronze = self._add_bronze_metadata(df, source_path)
        
        # Write to Iceberg table
        df_bronze.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(target_table)
        
        # Get row count
        row_count = self.spark.table(target_table).count()
        self.logger.info(f"✅ Ingested {row_count} merchants to {target_table}")
        
        return target_table
    
    def ingest_transactions(self, source_path: str, target_table: str = "spark_catalog.payments_bronze.transactions_raw"):
        """
        Ingest raw transaction data
        
        Args:
            source_path: Path to transaction CSV file
            target_table: Target Iceberg table name
        """
        self.logger.info(f"💳 Ingesting transactions from {source_path}")
        
        # Ensure database exists
        self.create_database("payments_bronze")
        
        # Create table if it doesn't exist
        self.create_transactions_table("payments_bronze")
        
        # Read CSV with schema inference
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        # Add bronze layer metadata
        df_bronze = self._add_bronze_metadata(df, source_path)
        
        # Write to Iceberg table with partitioning
        df_bronze.write \
            .format("iceberg") \
            .mode("overwrite") \
            .option("write-distribution-mode", "hash") \
            .saveAsTable(target_table)
        
        # Get row count
        row_count = self.spark.table(target_table).count()
        self.logger.info(f"✅ Ingested {row_count} transactions to {target_table}")
        
        return target_table
    
    def ingest_incremental_transactions(self, source_path: str, target_table: str = "spark_catalog.payments_bronze.transactions_raw"):
        """
        Ingest incremental transaction data (append mode)
        
        Args:
            source_path: Path to transaction CSV file
            target_table: Target Iceberg table name
        """
        self.logger.info(f"🔄 Ingesting incremental transactions from {source_path}")
        
        # Read CSV with schema inference
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        # Add bronze layer metadata
        df_bronze = self._add_bronze_metadata(df, source_path)
        
        # Append to existing table
        df_bronze.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(target_table)
        
        # Get row count
        row_count = self.spark.table(target_table).count()
        self.logger.info(f"✅ Appended transactions. Total rows: {row_count}")
        
        return target_table
    
    def _add_bronze_metadata(self, df: DataFrame, source_path: str) -> DataFrame:
        """
        Add bronze layer metadata columns
        
        Args:
            df: Source DataFrame
            source_path: Source file path
            
        Returns:
            DataFrame with bronze metadata columns
        """
        return df.withColumn("ingestion_timestamp", current_timestamp()) \
                 .withColumn("source_file", input_file_name()) \
                 .withColumn("bronze_layer_version", lit("1.0")) \
                 .withColumn("data_source", lit("payments_generator"))
    
    def ingest_batch(self, data_directory: str):
        """
        Ingest all data from a directory
        
        Args:
            data_directory: Directory containing CSV files
        """
        data_path = Path(data_directory)
        
        self.logger.info(f"📁 Starting batch ingestion from {data_directory}")
        
        # Find merchant files
        merchant_files = list(data_path.glob("merchants_*.csv"))
        if merchant_files:
            # Use the first merchant file (should be consistent)
            self.ingest_merchants(str(merchant_files[0]))
        
        # Find transaction files
        transaction_files = list(data_path.glob("transactions_*.csv"))
        
        # Process initial transaction file
        initial_files = [f for f in transaction_files if "initial" in f.name]
        if initial_files:
            self.ingest_transactions(str(initial_files[0]))
        
        # Process incremental transaction files
        incremental_files = [f for f in transaction_files if "initial" not in f.name]
        for file_path in incremental_files:
            self.ingest_incremental_transactions(str(file_path))
        
        self.logger.info("🎉 Batch ingestion completed")
    
    def validate_ingestion(self, table_name: str) -> Dict[str, any]:
        """
        Validate ingested data
        
        Args:
            table_name: Iceberg table name to validate
            
        Returns:
            Validation results dictionary
        """
        self.logger.info(f"🔍 Validating ingestion for {table_name}")
        
        df = self.spark.table(table_name)
        
        validation_results = {
            "table_name": table_name,
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "has_nulls": {},
            "data_types": {}
        }
        
        # Check for nulls in key columns
        key_columns = ["merchant_id"] if "merchant" in table_name else ["payment_id", "merchant_id"]
        for col_name in key_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                validation_results["has_nulls"][col_name] = null_count
        
        # Get data types
        for field in df.schema.fields:
            validation_results["data_types"][field.name] = str(field.dataType)
        
        self.logger.info(f"✅ Validation completed for {table_name}")
        return validation_results
    
    def get_table_info(self, table_name: str):
        """Get table information and statistics"""
        try:
            df = self.spark.table(table_name)
            
            self.logger.info(f"📊 Table Info: {table_name}")
            self.logger.info(f"   Rows: {df.count():,}")
            self.logger.info(f"   Columns: {len(df.columns)}")
            self.logger.info(f"   Schema:")
            
            for field in df.schema.fields:
                self.logger.info(f"     {field.name}: {field.dataType}")
            
            # Show sample data
            self.logger.info(f"   Sample data:")
            df.show(5, truncate=False)
            
        except Exception as e:
            self.logger.error(f"❌ Error getting table info: {e}")
    
    def cleanup_old_files(self, data_directory: str, keep_days: int = 30):
        """
        Clean up old data files after successful ingestion
        
        Args:
            data_directory: Directory containing data files
            keep_days: Number of days to keep files
        """
        self.logger.info(f"🧹 Cleaning up files older than {keep_days} days")
        
        data_path = Path(data_directory)
        cutoff_date = datetime.now().timestamp() - (keep_days * 24 * 60 * 60)
        
        files_deleted = 0
        for file_path in data_path.glob("*.csv"):
            if file_path.stat().st_mtime < cutoff_date:
                file_path.unlink()
                files_deleted += 1
                self.logger.info(f"   Deleted: {file_path.name}")
        
        self.logger.info(f"✅ Cleaned up {files_deleted} files")


def main():
    """Main entry point for bronze ingestion job"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Bronze Layer Ingestion Job')
    parser.add_argument('--data-dir', help='Directory containing CSV files')
    parser.add_argument('--validate', action='store_true', help='Validate ingestion')
    parser.add_argument('--cleanup', action='store_true', help='Clean up old files')
    parser.add_argument('--recreate-db', action='store_true', help='Recreate database with empty tables')
    
    args = parser.parse_args()
    
    # Initialize job
    job = BronzeIngestionJob()
    
    # Recreate database if requested
    if args.recreate_db:
        job.recreate_database("payments_bronze")
        job.logger.info("🎉 Database recreation completed successfully")
        return
    
    # Run ingestion if data directory provided
    if args.data_dir:
        job.ingest_batch(args.data_dir)
        
        # Validate if requested
        if args.validate:
            job.validate_ingestion("spark_catalog.payments_bronze.merchants_raw")
            job.validate_ingestion("spark_catalog.payments_bronze.transactions_raw")
        
        # Cleanup if requested
        if args.cleanup:
            job.cleanup_old_files(args.data_dir)
        
        job.logger.info("🎉 Bronze ingestion job completed successfully")
    else:
        job.logger.info("No data directory provided. Use --recreate-db to create empty tables or --data-dir to ingest data.")


if __name__ == "__main__":
    main()
