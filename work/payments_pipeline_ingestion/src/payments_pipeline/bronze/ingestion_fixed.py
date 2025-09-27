"""
Bronze Layer Ingestion - Fixed Version

ELT approach: Raw data ingestion with minimal transformation.
Uses pandas as workaround for Spark CSV reading issues.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, date

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, input_file_name, 
    regexp_extract, when, isnan, isnull
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from ..utils.spark import get_spark_session
from ..utils.config import PipelineConfig
from ..utils.logging import setup_logging


class BronzeIngestionJobFixed:
    """
    Bronze Layer Ingestion Job - Fixed Version
    
    Performs raw data ingestion with minimal transformation:
    - Uses pandas to read CSV files (workaround for Spark CSV issues)
    - Preserves original CSV structure
    - Adds metadata columns (ingestion_timestamp, source_file)
    - Basic data quality checks
    - Ingests into Iceberg tables
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self.logger = setup_logging(__name__)
        self.spark = get_spark_session()
        
        # Set up Iceberg catalog
        self._setup_iceberg_catalog()
    
    def _setup_iceberg_catalog(self):
        """Set up Iceberg catalog configuration"""
        try:
            # The catalog should already be configured via Spark config
            # Just create the namespace
            self.spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.payments_bronze")
            self.logger.info("✅ Iceberg catalog configured")
        except Exception as e:
            self.logger.warning(f"⚠️ Catalog setup warning: {e}")
            # Continue anyway as the catalog might already exist
    
    def _read_csv_with_pandas(self, source_path: str) -> DataFrame:
        """
        Read CSV file using pandas as workaround for Spark CSV issues
        
        Args:
            source_path: Path to CSV file
            
        Returns:
            Spark DataFrame
        """
        try:
            # Read with pandas
            df_pandas = pd.read_csv(source_path)
            self.logger.info(f"📁 Pandas read: {len(df_pandas)} rows, {len(df_pandas.columns)} columns")
            
            # Convert to Spark DataFrame
            df_spark = self.spark.createDataFrame(df_pandas)
            self.logger.info(f"🔄 Converted to Spark DataFrame: {df_spark.count()} rows")
            
            return df_spark
            
        except Exception as e:
            self.logger.error(f"❌ Error reading CSV with pandas: {e}")
            raise
    
    def ingest_merchants(self, source_path: str, target_table: str = "iceberg.payments_bronze.merchants_raw"):
        """
        Ingest raw merchant data
        
        Args:
            source_path: Path to merchant CSV file
            target_table: Target Iceberg table name
        """
        self.logger.info(f"🏪 Ingesting merchants from {source_path}")
        
        # Read CSV with pandas workaround
        df = self._read_csv_with_pandas(source_path)
        
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
    
    def ingest_transactions(self, source_path: str, target_table: str = "iceberg.payments_bronze.transactions_raw"):
        """
        Ingest raw transaction data
        
        Args:
            source_path: Path to transaction CSV file
            target_table: Target Iceberg table name
        """
        self.logger.info(f"💳 Ingesting transactions from {source_path}")
        
        # Read CSV with pandas workaround
        df = self._read_csv_with_pandas(source_path)
        
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
    
    def ingest_incremental_transactions(self, source_path: str, target_table: str = "iceberg.payments_bronze.transactions_raw"):
        """
        Ingest incremental transaction data (append mode)
        
        Args:
            source_path: Path to transaction CSV file
            target_table: Target Iceberg table name
        """
        self.logger.info(f"🔄 Ingesting incremental transactions from {source_path}")
        
        # Read CSV with pandas workaround
        df = self._read_csv_with_pandas(source_path)
        
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
                 .withColumn("source_file", lit(source_path)) \
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


def main():
    """Main entry point for bronze ingestion job"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Bronze Layer Ingestion Job (Fixed)')
    parser.add_argument('--data-dir', required=True, help='Directory containing CSV files')
    parser.add_argument('--validate', action='store_true', help='Validate ingestion')
    parser.add_argument('--cleanup', action='store_true', help='Clean up old files')
    
    args = parser.parse_args()
    
    # Initialize job
    job = BronzeIngestionJobFixed()
    
    # Run ingestion
    job.ingest_batch(args.data_dir)
    
    # Validate if requested
    if args.validate:
        job.validate_ingestion("iceberg.payments_bronze.merchants_raw")
        job.validate_ingestion("iceberg.payments_bronze.transactions_raw")
    
    # Cleanup if requested
    if args.cleanup:
        job.cleanup_old_files(args.data_dir)
    
    job.logger.info("🎉 Bronze ingestion job completed successfully")


if __name__ == "__main__":
    main()
