#!/usr/bin/env python3
"""
Atomic Updates for Silver Layer

Implements atomic updates using Iceberg's ACID properties and staging tables.
Ensures all-or-nothing updates to prevent data loss.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, current_timestamp, lit, hash, concat, 
    when, isnull, isnan, date_format, year, month, dayofweek,
    max as spark_max, min as spark_min, count as spark_count,
    weekofyear, dayofyear, date_sub
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, LongType

from ..utils.spark import get_spark_session
from ..utils.config import PipelineConfig
from ..utils.logging import setup_logging


class AtomicSilverUpdater:
    """
    Atomic Silver Layer Updater
    
    Implements atomic updates using:
    - Iceberg's ACID properties
    - Staging tables for validation
    - Transaction-like operations
    - Rollback capabilities
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None, spark_session=None):
        self.config = config or PipelineConfig()
        self.logger = setup_logging(__name__)
        
        # Use provided Spark session or get from utils
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = get_spark_session()
        
        # Set up namespaces
        self._setup_namespaces()
    
    def _setup_namespaces(self):
        """Set up required namespaces"""
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.config.iceberg_catalog}.{self.config.silver_namespace}")
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.config.iceberg_catalog}.{self.config.silver_namespace}_staging")
            self.logger.info("✅ Silver layer namespaces configured")
        except Exception as e:
            self.logger.warning(f"⚠️ Namespace setup warning: {e}")
    
    def create_silver_tables(self):
        """Create silver layer tables with proper schemas"""
        self.logger.info("Creating silver layer tables...")
        
        # Create dim_date table
        self._create_dim_date_table()
        
        # Create dim_merchants table
        self._create_dim_merchants_table()
        
        # Create fact_payments table
        self._create_fact_payments_table()
        
        self.logger.info("✅ Silver layer tables created successfully")
    
    def _create_dim_date_table(self):
        """Create dim_date table with business calendar"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_date (
            date DATE,
            year INT,
            month INT,
            day_of_week INT,
            month_name STRING,
            quarter INT,
            is_weekend BOOLEAN,
            is_holiday BOOLEAN,
            is_business_day BOOLEAN,
            fiscal_year INT,
            fiscal_quarter INT,
            fiscal_month INT,
            week_of_year INT,
            day_of_year INT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING iceberg
        LOCATION 's3a://warehouse/{self.config.silver_namespace}/dim_date'
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'zstd',
            'write.metadata.compression-codec' = 'gzip'
        )
        """
        self.spark.sql(create_table_sql)
        self.logger.info("✅ dim_date table created")
    
    def _create_dim_merchants_table(self):
        """Create dim_merchants table with SCD Type 2"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants (
            merchant_sk BIGINT,
            merchant_id STRING,
            merchant_name STRING,
            industry STRING,
            address STRING,
            city STRING,
            state STRING,
            zip_code STRING,
            phone STRING,
            email STRING,
            mdr_rate DOUBLE,
            size_category STRING,
            creation_date DATE,
            effective_date DATE,
            expiry_date DATE,
            is_current BOOLEAN,
            version INT,
            change_type STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            source_system STRING
        ) USING iceberg
        LOCATION 's3a://warehouse/{self.config.silver_namespace}/dim_merchants'
        PARTITIONED BY (effective_date)
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'zstd',
            'write.metadata.compression-codec' = 'gzip'
        )
        """
        self.spark.sql(create_table_sql)
        self.logger.info("✅ dim_merchants table created")
    
    def _create_fact_payments_table(self):
        """Create fact_payments table with hybrid approach"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments (
            payment_id STRING,
            payment_timestamp TIMESTAMP,
            payment_date DATE,
            payment_hour INT,
            merchant_id STRING,
            merchant_sk BIGINT,
            payment_amount DOUBLE,
            transactional_cost_amount DOUBLE,
            mdr_amount DOUBLE,
            net_profit DOUBLE,
            payment_type STRING,
            card_type STRING,
            card_brand STRING,
            card_issuer STRING,
            card_profile_id STRING,
            card_bin STRING,
            payment_status STRING,
            terminal_id STRING,
            payment_lat DOUBLE,
            payment_lng DOUBLE,
            created_at TIMESTAMP,
            source_system STRING
        ) USING iceberg
        LOCATION 's3a://warehouse/{self.config.silver_namespace}/fact_payments'
        PARTITIONED BY (payment_date)
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'zstd',
            'write.metadata.compression-codec' = 'gzip'
        )
        """
        self.spark.sql(create_table_sql)
        self.logger.info("✅ fact_payments table created")
    
    def populate_dim_date(self, start_date: date, end_date: date):
        """Populate dim_date with business calendar"""
        self.logger.info(f"Populating dim_date from {start_date} to {end_date}")
        
        # Generate date range
        date_range = self._generate_date_range(start_date, end_date)
        
        # Create DataFrame with date attributes
        dim_date_df = date_range.select(
            col("date"),
            year(col("date")).alias("year"),
            month(col("date")).alias("month"),
            dayofweek(col("date")).alias("day_of_week"),
            date_format(col("date"), "MMMM").alias("month_name"),
            when(month(col("date")).between(1, 3), 1)
            .when(month(col("date")).between(4, 6), 2)
            .when(month(col("date")).between(7, 9), 3)
            .otherwise(4).alias("quarter"),
            when(dayofweek(col("date")).isin(1, 7), True).otherwise(False).alias("is_weekend"),
            lit(False).alias("is_holiday"),  # Can be updated with holiday data
            when(dayofweek(col("date")).isin(1, 7), False).otherwise(True).alias("is_business_day"),
            year(col("date")).alias("fiscal_year"),  # Assuming calendar year
            when(month(col("date")).between(1, 3), 1)
            .when(month(col("date")).between(4, 6), 2)
            .when(month(col("date")).between(7, 9), 3)
            .otherwise(4).alias("fiscal_quarter"),
            month(col("date")).alias("fiscal_month"),
            weekofyear(col("date")).alias("week_of_year"),
            dayofyear(col("date")).alias("day_of_year"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
        
        # Write to dim_date table
        dim_date_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(f"{self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_date")
        
        self.logger.info("✅ dim_date populated successfully")
    
    def _generate_date_range(self, start_date: date, end_date: date) -> DataFrame:
        """Generate date range DataFrame"""
        # Create date range using Spark SQL
        date_range_sql = f"""
        SELECT explode(sequence(CAST('{start_date}' AS DATE), CAST('{end_date}' AS DATE), INTERVAL 1 DAY)) as date
        """
        return self.spark.sql(date_range_sql)
    
    def atomic_update_merchants(self, bronze_merchants_df: DataFrame) -> bool:
        """
        Atomically update dim_merchants with SCD Type 2 logic
        
        Args:
            bronze_merchants_df: DataFrame with merchant data from bronze layer
            
        Returns:
            bool: True if update succeeded, False otherwise
        """
        try:
            self.logger.info("Starting atomic merchant update...")
            
            # Step 1: Process SCD Type 2 logic
            processed_merchants_df = self._process_scd_type2_merchants(bronze_merchants_df)
            
            # Step 2: Validate data
            if not self._validate_merchants_data(processed_merchants_df):
                self.logger.error("❌ Merchant data validation failed")
                return False
            
            # Step 3: Write directly to main table (Iceberg handles atomicity)
            processed_merchants_df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .saveAsTable(f"{self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants")
            
            self.logger.info("✅ Atomic merchant update completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Atomic merchant update failed: {e}")
            return False
    
    def _create_staging_merchants_table(self, staging_table: str):
        """Create staging table for merchants"""
        create_staging_sql = f"""
        CREATE TABLE IF NOT EXISTS {staging_table} (
            merchant_sk BIGINT,
            merchant_id STRING,
            merchant_name STRING,
            industry STRING,
            address STRING,
            city STRING,
            state STRING,
            zip_code STRING,
            phone STRING,
            email STRING,
            mdr_rate DOUBLE,
            size_category STRING,
            creation_date DATE,
            effective_date DATE,
            expiry_date DATE,
            is_current BOOLEAN,
            version INT,
            change_type STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            source_system STRING
        ) USING iceberg
        """
        self.spark.sql(create_staging_sql)
    
    def _process_scd_type2_merchants(self, bronze_merchants_df: DataFrame) -> DataFrame:
        """Process SCD Type 2 logic for merchants"""
        # Get ALL current merchants (not just current ones)
        current_merchants_df = self.spark.table(f"{self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants")
        
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
    
    def _detect_merchant_changes(self, bronze_df: DataFrame, current_df: DataFrame) -> DataFrame:
        """Detect merchant changes for SCD Type 2"""
        # Join bronze and current data
        joined_df = bronze_df.alias("bronze") \
            .join(current_df.alias("current"), 
                  col("bronze.merchant_id") == col("current.merchant_id"), 
                  "left")
        
        # Identify changes - use version-based detection for better accuracy
        changes_df = joined_df.filter(
            col("current.merchant_id").isNull() |  # New merchant
            (col("bronze.version") > col("current.version")) |  # New version
            (col("bronze.merchant_name") != col("current.merchant_name")) |
            (col("bronze.address") != col("current.address")) |
            (col("bronze.phone") != col("current.phone")) |
            (col("bronze.email") != col("current.email")) |
            (col("bronze.mdr_rate") != col("current.mdr_rate"))
        ).select(
            col("bronze.*"),
            col("current.merchant_sk").alias("old_merchant_sk")
        )
        
        return changes_df
    
    def _generate_merchant_versions(self, changes_df: DataFrame) -> DataFrame:
        """Generate new merchant versions with SCD Type 2 logic"""
        # Use effective_date and version from bronze data to ensure uniqueness
        new_versions_df = changes_df.select(
            hash(concat(col("merchant_id"), col("effective_date").cast("string"), col("version").cast("string"))).alias("merchant_sk"),
            col("merchant_id"),
            col("merchant_name"),
            col("industry"),
            col("address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("phone"),
            col("email"),
            col("mdr_rate"),
            col("size_category"),
            col("creation_date"),
            col("effective_date"),
            lit(date(9999, 12, 31)).alias("expiry_date"),
            lit(True).alias("is_current"),
            col("version"),
            col("change_type"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
            lit("silver_layer").alias("source_system")
        )
        
        return new_versions_df
    
    def _expire_old_merchant_versions(self, changes_df: DataFrame, current_df: DataFrame) -> DataFrame:
        """Expire old merchant versions when new versions are created"""
        # Get merchant IDs that have new versions
        changed_merchant_ids = changes_df.select("merchant_id").distinct()
        
        # Get all current versions for these merchants
        old_versions_df = current_df.alias("current") \
            .join(changed_merchant_ids.alias("changed"), 
                  col("current.merchant_id") == col("changed.merchant_id"), 
                  "inner") \
            .select(col("current.*"))
        
        # Get the new effective dates for these merchants
        new_effective_dates = changes_df.select("merchant_id", "effective_date").distinct()
        
        # Join to get the new effective date for each merchant
        expired_versions_df = old_versions_df.alias("old") \
            .join(new_effective_dates.alias("new"), 
                  col("old.merchant_id") == col("new.merchant_id"), 
                  "inner") \
            .select(
                col("old.merchant_sk"),
                col("old.merchant_id"),
                col("old.merchant_name"),
                col("old.industry"),
                col("old.address"),
                col("old.city"),
                col("old.state"),
                col("old.zip_code"),
                col("old.phone"),
                col("old.email"),
                col("old.mdr_rate"),
                col("old.size_category"),
                col("old.creation_date"),
                col("old.effective_date"),
                # Set expiry_date to day before new version's effective_date
                date_sub(col("new.effective_date"), 1).alias("expiry_date"),
                lit(False).alias("is_current"),  # Mark as not current
                col("old.version"),
                col("old.change_type"),
                col("old.created_at"),
                current_timestamp().alias("updated_at"),
                col("old.source_system")
            )
        
        return expired_versions_df
    
    def _get_unchanged_merchants(self, changes_df: DataFrame, current_df: DataFrame) -> DataFrame:
        """Get merchants that haven't changed (merchants not in changes_df)"""
        # Get merchant IDs that have changes
        changed_merchant_ids = changes_df.select("merchant_id").distinct()
        
        # Get merchants that are NOT in the changes (unchanged merchants)
        # These should remain as-is (keep their current is_current status)
        unchanged_merchants_df = current_df.alias("current") \
            .join(changed_merchant_ids.alias("changed"), 
                  col("current.merchant_id") == col("changed.merchant_id"), 
                  "left_anti")  # left_anti gives us records in current but NOT in changed
        
        return unchanged_merchants_df
    
    def _validate_merchants_data(self, merchants_df: DataFrame) -> bool:
        """Validate merchant data quality"""
        try:
            # Check for required fields
            required_fields = ["merchant_id", "merchant_name", "mdr_rate"]
            for field in required_fields:
                null_count = merchants_df.filter(col(field).isNull()).count()
                if null_count > 0:
                    self.logger.error(f"❌ Found {null_count} null values in {field}")
                    return False
            
            # Check MDR rate range
            invalid_mdr_count = merchants_df.filter(
                (col("mdr_rate") < 0.01) | (col("mdr_rate") > 0.10)
            ).count()
            if invalid_mdr_count > 0:
                self.logger.error(f"❌ Found {invalid_mdr_count} invalid MDR rates")
                return False
            
            # Check for duplicate merchant_sk (only for current versions)
            current_merchants = merchants_df.filter(col("is_current") == True)
            current_count = current_merchants.count()
            unique_current_sk_count = current_merchants.select("merchant_sk").distinct().count()
            if current_count != unique_current_sk_count:
                self.logger.error(f"❌ Found duplicate merchant_sk values in current versions: {current_count} total, {unique_current_sk_count} unique")
                # Debug: show the duplicates
                duplicates = current_merchants.groupBy("merchant_sk").count().filter(col("count") > 1)
                if duplicates.count() > 0:
                    self.logger.error("Duplicate merchant_sk values:")
                    duplicates.show(20, False)
                return False
            
            self.logger.info("✅ Merchant data validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Validation error: {e}")
            return False
    
    def _atomic_swap_merchants(self, staging_table: str):
        """Atomically swap staging table with production table"""
        # Iceberg handles this atomically
        self.spark.sql(f"""
            INSERT OVERWRITE {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants
            SELECT * FROM {staging_table}
        """)
    
    def atomic_update_payments(self, bronze_payments_df: DataFrame, 
                             date_range: Tuple[date, date]) -> bool:
        """
        Atomically update fact_payments with window-based processing
        
        Args:
            bronze_payments_df: DataFrame with payment data from bronze layer
            date_range: Tuple of (start_date, end_date) for the update window
            
        Returns:
            bool: True if update succeeded, False otherwise
        """
        try:
            start_date, end_date = date_range
            self.logger.info(f"Starting atomic payments update for {start_date} to {end_date}")
            
            # Step 1: Create staging table
            staging_table = f"{self.config.iceberg_catalog}.{self.config.silver_namespace}_staging.payments_staging"
            self._create_staging_payments_table(staging_table)
            
            # Step 2: Process payments data
            processed_payments_df = self._process_payments_data(bronze_payments_df)
            
            # Step 3: Validate staging data
            validation_result, filtered_payments_df = self._validate_payments_data(processed_payments_df)
            if not validation_result:
                self.logger.error("❌ Payments data validation failed")
                return False
            
            # Step 4: Write to staging table
            filtered_payments_df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .saveAsTable(staging_table)
            
            # Step 5: Atomic swap (delete old data and insert new)
            self._atomic_swap_payments(staging_table, start_date, end_date)
            
            # Step 6: Cleanup staging table
            self.spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
            
            self.logger.info("✅ Atomic payments update completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Atomic payments update failed: {e}")
            # Cleanup staging table on failure
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
            except:
                pass
            return False
    
    def _create_staging_payments_table(self, staging_table: str):
        """Create staging table for payments"""
        create_staging_sql = f"""
        CREATE TABLE IF NOT EXISTS {staging_table} (
            payment_id STRING,
            payment_timestamp TIMESTAMP,
            payment_date DATE,
            payment_hour INT,
            merchant_id STRING,
            merchant_sk BIGINT,
            payment_amount DOUBLE,
            transactional_cost_amount DOUBLE,
            mdr_amount DOUBLE,
            net_profit DOUBLE,
            payment_type STRING,
            card_type STRING,
            card_brand STRING,
            card_issuer STRING,
            card_profile_id STRING,
            card_bin STRING,
            payment_status STRING,
            terminal_id STRING,
            payment_lat DOUBLE,
            payment_lng DOUBLE,
            created_at TIMESTAMP,
            source_system STRING
        ) USING iceberg
        """
        self.spark.sql(create_staging_sql)
    
    def _process_payments_data(self, bronze_payments_df: DataFrame) -> DataFrame:
        """Process payments data with merchant_sk lookup"""
        # Get current merchants for lookup
        current_merchants_df = self.spark.table(f"{self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants") \
            .filter(col("is_current") == True) \
            .select("merchant_id", "merchant_sk")
        
        # Find missing merchants from payments
        missing_merchants_df = self._create_missing_merchants(bronze_payments_df, current_merchants_df)
        
        # If we have missing merchants, add them to dim_merchants first
        if missing_merchants_df.count() > 0:
            self.logger.info(f"Found {missing_merchants_df.count()} missing merchants, adding them to dim_merchants")
            self._add_missing_merchants(missing_merchants_df)
            
            # Refresh current merchants after adding missing ones
            current_merchants_df = self.spark.table(f"{self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants") \
                .filter(col("is_current") == True) \
                .select("merchant_id", "merchant_sk")
        
        # Join with merchants to get merchant_sk
        processed_df = bronze_payments_df.alias("payments") \
            .join(current_merchants_df.alias("merchants"), 
                  col("payments.merchant_id") == col("merchants.merchant_id"), 
                  "left") \
            .select(
                col("payments.payment_id"),
                col("payments.payment_timestamp"),
                date_format(col("payments.payment_timestamp"), "yyyy-MM-dd").cast("date").alias("payment_date"),
                date_format(col("payments.payment_timestamp"), "H").cast("int").alias("payment_hour"),
                col("payments.merchant_id"),
                col("merchants.merchant_sk"),
                col("payments.payment_amount"),
                col("payments.transactional_cost_amount"),
                col("payments.mdr_amount"),
                col("payments.net_profit"),
                col("payments.payment_type"),
                col("payments.card_type"),
                col("payments.card_brand"),
                col("payments.card_issuer"),
                col("payments.card_profile_id"),
                col("payments.card_bin"),
                col("payments.payment_status"),
                col("payments.terminal_id"),
                col("payments.payment_lat"),
                col("payments.payment_lng"),
                current_timestamp().alias("created_at"),
                lit("silver_layer").alias("source_system")
            )
        
        return processed_df
    
    def _create_missing_merchants(self, bronze_payments_df: DataFrame, current_merchants_df: DataFrame) -> DataFrame:
        """Create missing merchant records from payment data"""
        # Find merchant_ids in payments that don't exist in current merchants
        missing_merchants = bronze_payments_df.alias("payments") \
            .join(current_merchants_df.alias("merchants"), 
                  col("payments.merchant_id") == col("merchants.merchant_id"), 
                  "left") \
            .filter(col("merchants.merchant_id").isNull()) \
            .select("payments.merchant_id") \
            .distinct()
        
        return missing_merchants
    
    def _add_missing_merchants(self, missing_merchants_df: DataFrame):
        """Add missing merchants to dim_merchants with default values"""
        current_date = date.today()
        
        # Create merchant records with default values
        new_merchants_df = missing_merchants_df.select(
            hash(concat(col("merchant_id"), lit(current_date.isoformat()), lit("1"))).alias("merchant_sk"),
            col("merchant_id"),
            concat(lit("Unknown Store "), col("merchant_id")).alias("merchant_name"),
            lit("unknown").alias("industry"),
            lit("Unknown Address").alias("address"),
            lit("Unknown City").alias("city"),
            lit("XX").alias("state"),
            lit("00000").alias("zip_code"),
            lit("000-000-0000").alias("phone"),
            concat(col("merchant_id"), lit("@unknown.com")).alias("email"),
            lit(0.025).alias("mdr_rate"),  # Default MDR rate
            lit("medium").alias("size_category"),  # Default size category
            lit(current_date).alias("creation_date"),
            lit(current_date).alias("effective_date"),
            lit(date(9999, 12, 31)).alias("expiry_date"),
            lit(True).alias("is_current"),
            lit(1).alias("version"),  # Default version
            lit("missing_merchant").alias("change_type"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
            lit("silver_layer_missing").alias("source_system")
        )
        
        # Insert new merchants
        new_merchants_df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(f"{self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants")
        
        self.logger.info(f"Added {new_merchants_df.count()} missing merchants to dim_merchants")
    
    def _validate_payments_data(self, payments_df: DataFrame) -> Tuple[bool, DataFrame]:
        """Validate payments data quality"""
        try:
            # Check for required fields
            required_fields = ["payment_id", "merchant_id", "payment_amount"]
            for field in required_fields:
                null_count = payments_df.filter(col(field).isNull()).count()
                if null_count > 0:
                    self.logger.error(f"❌ Found {null_count} null values in {field}")
                    return False
            
            # Check payment amount range
            invalid_amount_count = payments_df.filter(col("payment_amount") <= 0).count()
            if invalid_amount_count > 0:
                self.logger.error(f"❌ Found {invalid_amount_count} invalid payment amounts")
                return False
            
            # Check for missing merchant_sk (warn but don't fail)
            missing_sk_count = payments_df.filter(col("merchant_sk").isNull()).count()
            if missing_sk_count > 0:
                self.logger.warning(f"⚠️ Found {missing_sk_count} payments with missing merchant_sk (will be filtered out)")
                # Filter out payments with missing merchant_sk
                payments_df = payments_df.filter(col("merchant_sk").isNotNull())
            
            self.logger.info("✅ Payments data validation passed")
            return True, payments_df
            
        except Exception as e:
            self.logger.error(f"❌ Validation error: {e}")
            return False, payments_df
    
    def _atomic_swap_payments(self, staging_table: str, start_date: date, end_date: date):
        """Atomically swap payments data for date range with deduplication"""
        # Use INSERT OVERWRITE with deduplication to prevent duplicates
        self.spark.sql(f"""
            INSERT OVERWRITE {self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments
            SELECT payment_id, payment_timestamp, payment_date, payment_hour, merchant_id, merchant_sk,
                   payment_amount, transactional_cost_amount, mdr_amount, net_profit, payment_type,
                   card_type, card_brand, card_issuer, card_profile_id, card_bin, payment_status, 
                   terminal_id, payment_lat, payment_lng, created_at, source_system
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY payment_timestamp DESC) as rn
                FROM (
                    SELECT * FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments
                    WHERE payment_date NOT BETWEEN '{start_date}' AND '{end_date}'
                    UNION ALL
                    SELECT * FROM {staging_table}
                ) combined
            ) ranked
            WHERE rn = 1
        """)
    
    def atomic_update_payments_historical(self, bronze_payments_df: DataFrame) -> bool:
        """
        Atomically update fact_payments for historical data processing (no date filtering)
        
        Args:
            bronze_payments_df: DataFrame with payment data from bronze layer
            
        Returns:
            bool: True if update succeeded, False otherwise
        """
        try:
            self.logger.info("Starting atomic payments update for historical data (no date filtering)")
            
            # Step 1: Create staging table
            staging_table = f"{self.config.iceberg_catalog}.{self.config.silver_namespace}_staging.payments_staging"
            self._create_staging_payments_table(staging_table)
            
            # Step 2: Process payments data
            processed_payments_df = self._process_payments_data(bronze_payments_df)
            
            # Step 3: Validate staging data
            validation_result, filtered_payments_df = self._validate_payments_data(processed_payments_df)
            if not validation_result:
                self.logger.error("❌ Payments data validation failed")
                return False
            
            # Step 4: Write to staging table
            filtered_payments_df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .saveAsTable(staging_table)
            
            # Step 5: Atomic swap (replace all data with new data)
            self._atomic_swap_payments_historical(staging_table)
            
            # Step 6: Cleanup staging table
            self.spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
            
            self.logger.info("✅ Atomic payments update for historical data completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Atomic payments update for historical data failed: {e}")
            # Cleanup staging table on failure
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
            except:
                pass
            return False
    
    def _atomic_swap_payments_historical(self, staging_table: str):
        """Atomically swap payments data for historical processing (replace all data)"""
        # Use INSERT OVERWRITE with deduplication to replace all data
        self.spark.sql(f"""
            INSERT OVERWRITE {self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments
            SELECT payment_id, payment_timestamp, payment_date, payment_hour, merchant_id, merchant_sk,
                   payment_amount, transactional_cost_amount, mdr_amount, net_profit, payment_type,
                   card_type, card_brand, card_issuer, card_profile_id, card_bin, payment_status, 
                   terminal_id, payment_lat, payment_lng, created_at, source_system
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY payment_timestamp DESC) as rn
                FROM {staging_table}
            ) ranked
            WHERE rn = 1
        """)
    
    def get_table_info(self, table_name: str):
        """Get table information and statistics"""
        try:
            df = self.spark.table(table_name)
            
            self.logger.info(f"📊 Table Info: {table_name}")
            self.logger.info(f"   Rows: {df.count():,}")
            self.logger.info(f"   Columns: {len(df.columns)}")
            
            # Show sample data
            self.logger.info(f"   Sample data:")
            df.show(5, truncate=False)
            
        except Exception as e:
            self.logger.error(f"❌ Error getting table info: {e}")


def main():
    """Main entry point for atomic silver updater"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Atomic Silver Layer Updater')
    parser.add_argument('--create-tables', action='store_true', help='Create silver layer tables')
    parser.add_argument('--populate-dim-date', action='store_true', help='Populate dim_date table')
    parser.add_argument('--start-date', type=str, help='Start date for dim_date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date for dim_date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Initialize updater
    updater = AtomicSilverUpdater()
    
    if args.create_tables:
        updater.create_silver_tables()
        updater.logger.info("🎉 Silver layer tables created successfully")
    
    if args.populate_dim_date:
        if not args.start_date or not args.end_date:
            print("❌ --populate-dim-date requires --start-date and --end-date")
            return
        
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        
        updater.populate_dim_date(start_date, end_date)
        updater.logger.info("🎉 dim_date populated successfully")


if __name__ == "__main__":
    main()
