#!/usr/bin/env python3
"""
Silver Layer Ingestion Script

Standalone script to run silver layer processing from bronze data.
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from payments_pipeline.silver.silver_ingestion import SilverIngestionJob
from payments_pipeline.utils.spark import get_spark_session


def main():
    """Main entry point"""
    print("🚀 Starting Silver Layer Ingestion")
    
    # Initialize Spark session
    spark = get_spark_session()
    
    # Initialize silver ingestion job
    job = SilverIngestionJob(spark_session=spark)
    
    # Read bronze data
    print("📖 Reading bronze data...")
    bronze_merchants_df = spark.table("spark_catalog.payments_bronze.merchants_raw")
    bronze_payments_df = spark.table("spark_catalog.payments_bronze.transactions_raw")
    
    # Run silver layer pipeline
    print("🔄 Running silver layer pipeline...")
    success = job.run_complete_silver_pipeline(
        bronze_merchants_df,
        bronze_payments_df,
        "daily"  # Default to daily processing
    )
    
    if success:
        print("🎉 Silver layer ingestion completed successfully")
        
        # Show statistics
        stats = job.get_silver_layer_stats()
        print("\n📊 Silver Layer Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    else:
        print("❌ Silver layer ingestion failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
