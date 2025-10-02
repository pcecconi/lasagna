#!/usr/bin/env python3
"""
Container Setup Helper
This script helps set up the payments pipeline in the container environment
"""

import os
import sys
from pathlib import Path

def check_container_environment():
    """Check if we're running in the container"""
    # Check for container-specific environment variables
    container_indicators = [
        'SPARK_HOME',
        'JUPYTER_ENABLE_LAB',
        'PYSPARK_DRIVER_PYTHON'
    ]
    
    is_container = any(os.environ.get(var) for var in container_indicators)
    
    if is_container:
        print("🐳 Running in container environment")
        return True
    else:
        print("🏠 Running on host machine")
        return False

def check_raw_data_availability():
    """Check if raw data files are available"""
    raw_data_path = Path("/usr/local/spark_dev/work/payments_data_source/raw_data")
    
    if raw_data_path.exists():
        csv_files = list(raw_data_path.glob("*.csv"))
        print(f"📁 Found {len(csv_files)} CSV files in raw_data directory")
        
        for file in csv_files:
            print(f"   - {file.name}")
        
        return True
    else:
        print("⚠️  Raw data directory not found")
        print(f"   Expected path: {raw_data_path}")
        return False

def check_spark_session():
    """Check Spark session configuration"""
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("PaymentsPipelineSetup") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hive") \
            .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
            .getOrCreate()
        
        print("✅ Spark session configured successfully")
        print(f"   Spark version: {spark.version}")
        print(f"   Master: {spark.conf.get('spark.master')}")
        
        # Test Iceberg catalog
        try:
            spark.sql("SHOW NAMESPACES IN iceberg").show()
            print("✅ Iceberg catalog accessible")
        except Exception as e:
            print(f"⚠️  Iceberg catalog issue: {e}")
        
        return True
        
    except ImportError:
        print("❌ PySpark not available")
        return False
    except Exception as e:
        print(f"❌ Spark session error: {e}")
        return False

def main():
    """Main setup check"""
    print("🔧 Payments Pipeline - Container Setup Check")
    print("=" * 50)
    
    # Check environment
    is_container = check_container_environment()
    
    # Check raw data
    has_data = check_raw_data_availability()
    
    # Check Spark (only in container)
    if is_container:
        spark_ok = check_spark_session()
    else:
        print("ℹ️  Spark check skipped (host environment)")
        spark_ok = True
    
    print("\n📋 Setup Summary:")
    print(f"   Environment: {'Container' if is_container else 'Host'}")
    print(f"   Raw Data: {'✅ Available' if has_data else '❌ Missing'}")
    print(f"   Spark: {'✅ Ready' if spark_ok else '❌ Issues'}")
    
    if is_container and has_data and spark_ok:
        print("\n🎉 Container setup complete! Ready for data processing.")
    elif not is_container:
        print("\n📝 Host setup complete! Ready for data generation.")
    else:
        print("\n⚠️  Setup incomplete. Check the issues above.")

if __name__ == "__main__":
    main()

