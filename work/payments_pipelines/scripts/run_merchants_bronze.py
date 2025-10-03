#!/usr/bin/env python3
"""
Run Merchants Bronze Pipeline

This script runs the new modular MerchantsBronzePipeline to ingest merchant data
into the bronze layer using the modular architecture.

Key Features:
- Creates 'merchants' table in 'payments_bronze' database (coexists with legacy)
- Uses modular architecture components with dependency management
- Depends on CSVUploaderPipeline for file availability
- Comprehensive data quality checks
- Configurable and reusable
- Production-ready implementation
"""

import sys
import logging
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from payments_pipeline.common.pipeline_orchestrator import PipelineOrchestrator
from payments_pipeline.bronze.merchants_pipeline import MerchantsBronzePipeline
from payments_pipeline.bronze.csv_uploader_pipeline import CSVUploaderPipeline


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def create_spark_session():
    """Create Spark session for the example"""
    return SparkSession.builder \
        .appName("ModularMerchantsExample") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "/tmp/iceberg-warehouse") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def run_direct_pipeline_example():
    """Example: Run the modular merchants pipeline directly"""
    print("🚀 Running Direct Pipeline Example")
    print("=" * 50)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Configuration for the new modular pipeline
    config = {
        "catalog": "iceberg",
        "database": "payments_bronze",
        "table_name": "merchants",  # Different from legacy table
        "source_config": {
            "base_path": "/opt/workspace/work/payments_data_source/raw_data",
            "file_pattern": "merchants_*.csv"
        },
        "quality_checks": ["required_columns", "null_values", "duplicates"],
        "batch_size": 1000,
        "enable_debug_logging": True
    }
    
    # Create and run the pipeline
    pipeline = MerchantsBronzePipeline(config, spark)
    result = pipeline.execute()
    
    if result.success:
        print(f"✅ Pipeline completed successfully: {result.message}")
        print(f"📊 Metrics: {pipeline.get_pipeline_metrics()}")
    else:
        print(f"❌ Pipeline failed: {result.message}")
    
    return result.success


def run_orchestrator_example():
    """Example: Run using the pipeline orchestrator"""
    print("\n🎭 Running Orchestrator Example")
    print("=" * 50)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Initialize orchestrator
    orchestrator = PipelineOrchestrator(spark)
    
    # Register pipeline classes (dependencies first)
    orchestrator.register_pipeline_class("CSVUploaderPipeline", CSVUploaderPipeline)
    orchestrator.register_pipeline_class("MerchantsBronzePipeline", MerchantsBronzePipeline)
    
    # Load configuration from YAML
    config_path = Path(__file__).parent.parent / "pipeline_configs" / "development.yml"
    orchestrator.load_configuration(str(config_path))
    
    # Execute the modular merchants pipeline
    result = orchestrator.execute_single_pipeline("modular_merchants_bronze")
    
    if result.success:
        print(f"✅ Orchestrator pipeline completed: {result.message}")
    else:
        print(f"❌ Orchestrator pipeline failed: {result.message}")
    
    return result.success


def compare_with_legacy_table():
    """Compare the new modular table with the legacy table"""
    print("\n🔍 Comparing with Legacy Table")
    print("=" * 50)
    
    spark = create_spark_session()
    
    try:
        # Check if legacy table exists
        legacy_exists = False
        try:
            legacy_df = spark.table("iceberg.payments_bronze.merchants_raw")
            legacy_count = legacy_df.count()
            legacy_exists = True
            print(f"📊 Legacy table 'merchants_raw': {legacy_count} records")
        except Exception as e:
            print(f"⚠️ Legacy table 'merchants_raw' not found: {e}")
        
        # Check new modular table
        try:
            modular_df = spark.table("iceberg.payments_bronze.merchants")
            modular_count = modular_df.count()
            print(f"📊 Modular table 'merchants': {modular_count} records")
            
            if legacy_exists:
                if legacy_count == modular_count:
                    print("✅ Both tables have the same number of records!")
                else:
                    print(f"⚠️ Record count differs: Legacy={legacy_count}, Modular={modular_count}")
            
            # Show sample data from modular table
            print("\n📋 Sample data from modular 'merchants' table:")
            modular_df.select("merchant_id", "merchant_name", "merchant_category", "merchant_country") \
                     .limit(5) \
                     .show(truncate=False)
                     
        except Exception as e:
            print(f"❌ Modular table 'merchants' not found: {e}")
    
    except Exception as e:
        print(f"❌ Error comparing tables: {e}")


def main():
    """Main function to run all examples"""
    setup_logging()
    
    print("🏪 Modular Merchants Bronze Pipeline Example")
    print("=" * 60)
    print("This example demonstrates the new modular architecture")
    print("for merchant data ingestion, creating an independent")
    print("'merchants' table that coexists with the legacy pipeline.")
    print("=" * 60)
    
    try:
        # Run direct pipeline example
        direct_success = run_direct_pipeline_example()
        
        # Run orchestrator example (optional)
        orchestrator_success = run_orchestrator_example()
        
        # Compare with legacy table
        compare_with_legacy_table()
        
        # Summary
        print("\n📋 Summary")
        print("=" * 30)
        print(f"Direct Pipeline: {'✅ Success' if direct_success else '❌ Failed'}")
        print(f"Orchestrator: {'✅ Success' if orchestrator_success else '❌ Failed'}")
        
        if direct_success or orchestrator_success:
            print("\n🎉 Modular merchants pipeline is working!")
            print("📊 Check the 'iceberg.payments_bronze.merchants' table")
            print("🔄 Both legacy and modular pipelines can coexist")
        else:
            print("\n❌ Pipeline execution failed")
            
    except Exception as e:
        print(f"❌ Example failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
