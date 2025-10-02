#!/usr/bin/env python3
"""
Silver Layer Ingestion Script

Standalone script to run silver layer processing from bronze data.
"""

import sys
import os
import argparse
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from payments_pipeline.silver.silver_ingestion import SilverIngestionJob
from payments_pipeline.utils.spark import get_spark_session
from payments_pipeline.utils.config import PipelineConfig


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Silver Layer Ingestion Script')
    parser.add_argument('--processing-window', choices=['daily', 'weekly', 'monthly', 'historical'], 
                       default='daily', help='Processing window')
    parser.add_argument('--historical-mode', action='store_true', 
                       help='Process all data without date filtering (for historical data)')
    parser.add_argument('--stats', action='store_true', help='Show silver layer statistics only')
    
    args = parser.parse_args()
    
    print("🚀 Starting Silver Layer Ingestion")
    
    # Initialize configuration
    config = PipelineConfig()
    
    # Initialize Spark session
    spark = get_spark_session()
    
    # Initialize silver ingestion job
    job = SilverIngestionJob(spark_session=spark)
    
    # Handle stats-only mode
    if args.stats:
        stats = job.get_silver_layer_stats()
        print("\n📊 Silver Layer Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        return
    
    # Create silver layer tables if they don't exist
    print("🏗️ Creating silver layer tables if needed...")
    job.atomic_updater.create_silver_tables()
    
    # Read bronze data
    print("📖 Reading bronze data...")
    bronze_merchants_df = spark.table(f"{config.iceberg_catalog}.{config.bronze_namespace}.merchants_raw")
    bronze_payments_df = spark.table(f"{config.iceberg_catalog}.{config.bronze_namespace}.transactions_raw")
    
    # Run silver layer pipeline
    processing_mode = "historical" if args.historical_mode else args.processing_window
    print(f"🔄 Running silver layer pipeline ({processing_mode})...")
    success = job.run_complete_silver_pipeline(
        bronze_merchants_df,
        bronze_payments_df,
        args.processing_window,
        args.historical_mode
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

