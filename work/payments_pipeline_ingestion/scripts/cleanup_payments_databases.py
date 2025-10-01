#!/usr/bin/env python3
"""
Cleanup Payments Databases Script

Removes all payments databases and their tables.
Use this script to clean up before recreating databases.

Usage:
    python scripts/cleanup_payments_databases.py [--force] [--bronze-only] [--silver-only]
    
Options:
    --force         Skip confirmation prompt
    --bronze-only   Only remove bronze layer
    --silver-only   Only remove silver layer
"""

import argparse
import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from payments_pipeline.utils.spark import get_spark_session


def cleanup_database(spark, db_name, force=False):
    """Clean up a specific database using safe Spark SQL operations"""
    try:
        print(f"Cleaning up database: {db_name}")
        
        # List tables in the database
        try:
            tables_df = spark.sql(f'SHOW TABLES IN {db_name}')
            tables = [row[1] for row in tables_df.collect()]  # Use index for table name
            print(f"  Found {len(tables)} tables to drop: {tables}")
        except Exception as e:
            print(f"  ⚠️ Could not list tables in {db_name}: {e}")
            tables = []
        
        # Drop all tables first
        for table_name in tables:
            print(f"    Dropping table: {db_name}.{table_name}")
            try:
                spark.sql(f'DROP TABLE IF EXISTS {db_name}.{table_name}')
                print(f"      ✅ Successfully dropped {db_name}.{table_name}")
            except Exception as e:
                print(f"      ⚠️ Failed to drop {db_name}.{table_name}: {e}")
        
        # Drop the database using CASCADE
        print(f"  Dropping database: {db_name}")
        try:
            spark.sql(f'DROP DATABASE IF EXISTS {db_name} CASCADE')
            print(f"  ✅ Successfully dropped database: {db_name}")
            return True
        except Exception as e:
            print(f"  ❌ Failed to drop database {db_name}: {e}")
            return False
        
    except Exception as e:
        print(f"  ❌ Failed to clean up {db_name}: {e}")
        return False


def main():
    """Main cleanup function"""
    parser = argparse.ArgumentParser(description='Cleanup Payments Databases')
    parser.add_argument('--force', action='store_true', 
                       help='Skip confirmation prompt')
    parser.add_argument('--bronze-only', action='store_true', 
                       help='Only remove bronze layer')
    parser.add_argument('--silver-only', action='store_true', 
                       help='Only remove silver layer')
    
    args = parser.parse_args()
    
    print("🧹 Cleaning up Payments Databases...")
    print("=" * 50)
    
    # Initialize Spark
    spark = get_spark_session()
    
    # Determine which databases to clean up
    databases_to_clean = []
    
    if args.bronze_only:
        databases_to_clean = ['payments_bronze']
    elif args.silver_only:
        databases_to_clean = ['payments_silver', 'payments_silver_staging']
    else:
        databases_to_clean = ['payments_bronze', 'payments_silver', 'payments_silver_staging']
    
    # Check which databases actually exist
    existing_databases = spark.sql('SHOW DATABASES').collect()
    existing_db_names = [row[0] for row in existing_databases]  # Use index instead of column name
    
    databases_to_clean = [db for db in databases_to_clean if db in existing_db_names]
    
    if not databases_to_clean:
        print("✅ No payments databases found to clean up")
        return
    
    print(f"Found {len(databases_to_clean)} databases to clean up:")
    for db in databases_to_clean:
        print(f"  - {db}")
    
    # Confirmation prompt
    if not args.force:
        response = input("\nAre you sure you want to delete these databases? (y/N): ")
        if response.lower() != 'y':
            print("❌ Cleanup cancelled")
            return
    
    # Clean up databases
    success_count = 0
    for db in databases_to_clean:
        if cleanup_database(spark, db, args.force):
            success_count += 1
    
    print("\n" + "=" * 50)
    print(f"🎉 Cleanup completed! {success_count}/{len(databases_to_clean)} databases removed")
    print("=" * 50)
    
    if success_count == len(databases_to_clean):
        print("✅ All payments databases successfully removed")
    else:
        print("⚠️  Some databases could not be removed")
        sys.exit(1)


if __name__ == "__main__":
    main()



