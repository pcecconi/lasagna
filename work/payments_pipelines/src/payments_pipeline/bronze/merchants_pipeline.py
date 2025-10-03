#!/usr/bin/env python3
"""
Merchants Bronze Ingestion Pipeline

Independent modular pipeline for ingesting merchant data into the bronze layer.
Uses the new modular architecture and creates a separate 'merchants' table
to coexist with the legacy pipeline.
"""

import logging
from typing import Dict, Any, List
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType

from ..common.base_pipeline import BasePipeline, DataIngestionMixin, TableManagementMixin, DataQualityMixin
from ..common.data_quality import (
    DataQualityFramework,
    RequiredColumnsCheck,
    NullValuesCheck,
    RowCountCheck,
    DuplicatesCheck,
    RangeCheck
)
from ..common.schema_manager import SchemaManager


class MerchantsBronzePipeline(BasePipeline, DataIngestionMixin, TableManagementMixin, DataQualityMixin):
    """
    Modular Merchants Bronze Ingestion Pipeline
    
    Features:
    - Independent from legacy pipeline
    - Creates 'merchants' table in 'payments_bronze' database
    - Uses modular architecture components
    - Comprehensive data quality checks
    - Configurable source paths and settings
    """
    
    def __init__(self, config: Dict[str, Any], spark_session: SparkSession, pipeline_name: str = None):
        super().__init__(
            config=config,
            spark_session=spark_session,
            pipeline_name=pipeline_name or "MerchantsBronzePipeline"
        )
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Initialize modular components
        self.schema_manager = SchemaManager(spark_session)
        self.quality_framework = DataQualityFramework(spark_session)
        
        # Pipeline-specific settings
        self.catalog = self.config.get("catalog", "iceberg")
        self.database = self.config.get("database", "payments_bronze")
        self.table_name = self.config.get("table_name", "merchants")
        self.full_table_name = f"{self.catalog}.{self.database}.{self.table_name}"
        
        # Source configuration
        self.source_config = self.config.get("source_config", {})
        self.base_path = self.source_config.get("base_path", "/opt/workspace/work/payments_data_source/raw_data")
        self.file_pattern = self.source_config.get("file_pattern", "merchants_*.csv")
        
        # S3 marker configuration for tracking uploaded and processed files
        self.s3_status_prefix = self.config.get("s3_status_prefix", "s3a://warehouse/pipeline_status/")
        
        self.logger.info(f"🚀 Initialized MerchantsBronzePipeline for table: {self.full_table_name}")
    
    def execute(self) -> bool:
        """
        Execute the merchants bronze ingestion pipeline
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info("🏪 Starting Merchants Bronze Ingestion Pipeline")
            
            # Step 1: Validate inputs
            if not self.validate_inputs():
                return self._end_execution(False, "Input validation failed")
            
            # Step 2: Create namespace if needed
            self.create_namespace(self.database)
            
            # Step 3: Load and process merchant data
            merchants_df, processed_files = self._load_merchant_data()
            if merchants_df is None:
                return self._end_execution(False, "Failed to load merchant data")
            
            # Step 4: Add metadata
            enriched_df = self.add_metadata(merchants_df, self.base_path, "bronze")
            
            # Step 5: Data quality checks
            if not self._run_data_quality_checks(enriched_df):
                return self._end_execution(False, "Data quality checks failed")
            
            # Step 6: Create or replace table
            if not self._create_merchants_table():
                return self._end_execution(False, "Failed to create merchants table")
            
            # Step 7: Write data
            self._write_merchants_data(enriched_df, processed_files)
            
            # Step 8: Verify results
            if not self._verify_ingestion():
                return self._end_execution(False, "Ingestion verification failed")
            
            self.logger.info("✅ Merchants Bronze Ingestion Pipeline completed successfully")
            return self._end_execution(True, f"Successfully ingested merchants data to {self.full_table_name}")
            
        except Exception as e:
            self.logger.error(f"❌ Merchants Bronze Ingestion Pipeline failed: {e}")
            return self._end_execution(False, f"Pipeline failed with error: {str(e)}")
    
    def validate_inputs(self) -> bool:
        """Validate pipeline inputs and configuration"""
        try:
            self.logger.info("🔍 Validating pipeline inputs...")
            
            # Check required configuration
            required_configs = ["catalog", "database", "table_name"]
            for config_key in required_configs:
                if config_key not in self.config:
                    self.logger.error(f"Missing required configuration: {config_key}")
                    return False
            
            # Check source path exists
            source_path = Path(self.base_path)
            if not source_path.exists():
                self.logger.error(f"Source path does not exist: {source_path}")
                return False
            
            # Check for merchant files
            merchant_files = list(source_path.glob(self.file_pattern))
            if not merchant_files:
                self.logger.error(f"No merchant files found matching pattern: {self.file_pattern}")
                return False
            
            self.logger.info(f"✅ Found {len(merchant_files)} merchant files")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Input validation failed: {e}")
            return False
    
    def _load_merchant_data(self) -> tuple[DataFrame, List[str]]:
        """Load merchant data from S3 files that have been uploaded but not yet processed"""
        try:
            self.logger.info("📥 Loading merchant data from S3 files...")
            
            # Find files that need processing (uploaded but not processed)
            s3_files_to_process = self._get_s3_files_to_process()
            if not s3_files_to_process:
                self.logger.info("ℹ️ No new files to process")
                return None, []
            
            self.logger.info(f"📄 Processing {len(s3_files_to_process)} files from S3")
            
            # Extract filenames from S3 paths for processing markers
            processed_filenames = [path.split('/')[-1] for path in s3_files_to_process]
            
            # Define merchant schema
            merchant_schema = self.schema_manager.create_merchants_schema()
            
            # Read CSV files from S3
            merchants_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(merchant_schema) \
                .csv(s3_files_to_process)  # Pass list of S3 paths
            
            # Basic data cleaning
            merchants_df = merchants_df.filter(col("merchant_id").isNotNull())
            
            # Add bronze layer metadata (same as legacy pipeline)
            from pyspark.sql.functions import current_timestamp, input_file_name, lit
            merchants_df = merchants_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                     .withColumn("source_file", input_file_name()) \
                                     .withColumn("bronze_layer_version", lit("1.0")) \
                                     .withColumn("data_source", lit("payments_generator"))
            
            row_count = merchants_df.count()
            self.logger.info(f"✅ Loaded {row_count} merchant records from S3")
            self.logger.info(f"📊 Note: Overlapping merchant records are expected for SCD Type 2 processing")
            
            return merchants_df, processed_filenames
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load merchant data from S3: {e}")
            return None, []
    
    def _get_s3_files_to_process(self) -> List[str]:
        """Get list of S3 files that have been uploaded but not yet processed"""
        try:
            # Get all uploaded files from S3 markers
            uploaded_files = self._get_uploaded_files()
            
            s3_files_to_process = []
            for filename in uploaded_files:
                # Check if file was already processed (has processing marker)
                if not self._has_processing_marker(filename):
                    # Construct S3 path for the file
                    s3_path = f"s3a://warehouse/payments/{filename}"
                    s3_files_to_process.append(s3_path)
            
            return s3_files_to_process
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get S3 files to process: {e}")
            return []
    
    def _get_uploaded_files(self) -> List[str]:
        """Get list of files that have been uploaded (have upload markers)"""
        try:
            # List all upload markers in S3
            markers_path = f"{self.s3_status_prefix}*.uploaded"
            
            # Try to read marker files
            try:
                marker_df = self.spark.read.format("json").load(markers_path)
                if marker_df.count() > 0:
                    # Extract filenames from markers
                    filenames = [row.filename for row in marker_df.select("filename").collect()]
                    # Filter for merchant files only
                    merchant_files = [f for f in filenames if f.startswith("merchants_") and f.endswith(".csv")]
                    return merchant_files
                else:
                    return []
            except Exception:
                return []
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get uploaded files: {e}")
            return []
    
    def _has_upload_marker(self, filename: str) -> bool:
        """Check if file has an upload marker in S3"""
        try:
            marker_path = f"{self.s3_status_prefix}{filename}.uploaded"
            marker_df = self.spark.read.format("json").load(marker_path)
            return marker_df.count() > 0
        except Exception:
            return False
    
    def _has_processing_marker(self, filename: str) -> bool:
        """Check if file has a processing marker in S3"""
        try:
            marker_path = f"{self.s3_status_prefix}{filename}.processed"
            marker_df = self.spark.read.format("json").load(marker_path)
            count = marker_df.count()
            return count > 0
        except Exception:
            return False
    
    def _create_processing_marker(self, filename: str, row_count: int):
        """Create a processing marker file in S3"""
        try:
            from datetime import datetime
            import json
            
            marker_data = {
                "filename": filename,
                "processed_at": datetime.now().isoformat(),
                "rows_processed": row_count,
                "status": "processed",
                "pipeline": self.pipeline_name
            }
            
            # Create marker file in S3
            marker_path = f"{self.s3_status_prefix}{filename}.processed"
            
            # Use Spark to write the marker file
            marker_df = self.spark.createDataFrame([marker_data])
            marker_df.coalesce(1).write.mode("overwrite").format("json").save(marker_path)
            
            self.logger.debug(f"Created processing marker: {marker_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to create processing marker for {filename}: {e}")
    
    def _run_data_quality_checks(self, df: DataFrame) -> bool:
        """Run comprehensive data quality checks"""
        try:
            self.logger.info("🔍 Running data quality checks...")
            
            # Get quality checks from configuration
            quality_checks = self.config.get("quality_checks", ["required_columns", "null_values", "row_count"])
            
            # Add configured quality checks
            if "required_columns" in quality_checks:
                self.quality_framework.add_check(RequiredColumnsCheck(["merchant_id", "merchant_name", "size_category", "status"]))
            if "null_values" in quality_checks:
                self.quality_framework.add_check(NullValuesCheck(["merchant_id", "merchant_name"]))
            if "row_count" in quality_checks:
                self.quality_framework.add_check(RowCountCheck(min_rows=1))
            if "duplicates" in quality_checks:
                self.quality_framework.add_check(DuplicatesCheck(["merchant_id"]))
            if "range_check_mdr_rate" in quality_checks:
                self.quality_framework.add_check(RangeCheck("mdr_rate", min_value=0.01, max_value=0.10))
            
            # Run quality checks
            results = self.quality_framework.run_checks(df)
            
            # Check results
            summary = results["summary"]
            if summary["failed"] > 0:
                self.logger.error(f"❌ Data quality checks failed: {summary['failed']} failures")
                self.logger.error(self.quality_framework.generate_report(results))
                return False
            
            self.logger.info(f"✅ Data quality checks passed: {summary['passed']} checks")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Data quality checks failed with error: {e}")
            return False
    
    def _create_merchants_table(self) -> bool:
        """Create merchants table using same approach as legacy pipeline"""
        try:
            self.logger.info("🏗️ Creating merchants table...")
            
            # Create database if it doesn't exist
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.database}")
            
            # Create table using SQL DDL (same as legacy pipeline)
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.full_table_name} (
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
                effective_date DATE,
                status STRING,
                last_transaction_date STRING,
                version INT,
                change_type STRING,
                churn_date DATE,
                ingestion_timestamp TIMESTAMP,
                source_file STRING,
                bronze_layer_version STRING,
                data_source STRING
            ) USING iceberg
            LOCATION 's3a://warehouse/{self.database}.db/{self.table_name}'
            TBLPROPERTIES (
                'write.parquet.compression-codec' = 'zstd',
                'write.metadata.compression-codec' = 'gzip'
            )
            """
            
            self.spark.sql(create_table_sql)
            self.logger.info(f"✅ Table {self.full_table_name} created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to create merchants table: {e}")
            return False
    
    def _write_merchants_data(self, df: DataFrame, processed_files: List[str]) -> None:
        """Write merchant data to the table"""
        try:
            self.logger.info("💾 Writing merchant data to table...")
            
            # Write data with append mode (same as legacy pipeline)
            df.write \
                .format("iceberg") \
                .mode("append") \
                .saveAsTable(self.full_table_name)
            
            # Get row count for processing markers
            row_count = df.count()
            
            # Create processing markers ONLY for files that were actually processed in this run
            for filename in processed_files:
                if not self._has_processing_marker(filename):
                    self._create_processing_marker(filename, row_count)
                    self.logger.info(f"📄 Created processing marker for {filename}")
            
            self.logger.info("✅ Merchant data written successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to write merchant data: {e}")
            raise
    
    def _verify_ingestion(self) -> bool:
        """Verify the ingestion was successful"""
        try:
            self.logger.info("🔍 Verifying ingestion results...")
            
            # Check table exists and has data
            if not self.verify_table(self.full_table_name):
                return False
            
            # Get row count
            row_count = self.spark.table(self.full_table_name).count()
            
            if row_count == 0:
                self.logger.error("❌ No data found in merchants table")
                return False
            
            # Get table info
            table_info = self.get_table_info(self.full_table_name)
            
            self.logger.info(f"✅ Ingestion verified: {row_count} records in {self.full_table_name}")
            self.logger.info(f"📊 Table created: {table_info['table_name']} with {table_info['row_count']} records")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ingestion verification failed: {e}")
            return False
    
    def get_pipeline_metrics(self) -> Dict[str, Any]:
        """Get pipeline-specific metrics"""
        try:
            if self.verify_table(self.full_table_name):
                row_count = self.spark.table(self.full_table_name).count()
                return {
                    "merchants_ingested": row_count,
                    "table_name": self.full_table_name,
                    "source_path": self.base_path,
                    "file_pattern": self.file_pattern
                }
            else:
                return {"merchants_ingested": 0, "table_name": self.full_table_name}
        except Exception as e:
            self.logger.error(f"Error getting metrics: {e}")
            return {"merchants_ingested": 0, "error": str(e)}
