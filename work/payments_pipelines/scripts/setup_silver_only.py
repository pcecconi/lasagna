#!/usr/bin/env python3
"""
Setup Silver Layer Only Script

Creates only the silver layer without bronze ingestion.
This is useful for testing the silver layer functionality.

Usage:
    python scripts/setup_silver_only.py
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from payments_pipeline.silver.silver_ingestion import SilverIngestionJob
from payments_pipeline.utils.spark import get_spark_session
from payments_pipeline.utils.config import PipelineConfig
from datetime import date


def main():
    """Main setup function"""
    print("🚀 Setting up Silver Layer Only...")
    print("=" * 50)
    
    # Initialize components
    config = PipelineConfig()
    spark = get_spark_session()
    
    try:
        # Set up silver layer
        print("🎯 Creating Silver Layer...")
        silver_job = SilverIngestionJob(config, spark)
        
        # Create silver tables
        silver_job.atomic_updater.create_silver_tables()
        print("✅ Silver tables created successfully")
        
        # Populate date dimension
        print("📅 Populating date dimension...")
        silver_job.atomic_updater.populate_dim_date(date(2024, 1, 1), date(2024, 12, 31))
        print("✅ Date dimension populated")
        
        print("\n" + "=" * 50)
        print("🎉 Silver layer setup completed successfully!")
        print("=" * 50)
        
        print("\n📋 Setup Summary:")
        print("  ✅ Silver layer created")
        print("  ✅ Date dimension populated")
        
        print("\n💡 Next steps:")
        print("  - Run data quality checks: python scripts/run_data_quality.py")
        print("  - View data in notebooks: jupyter lab")
        print("  - Test with sample data: create bronze layer first")
        
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()



