#!/usr/bin/env python3
"""
Force cleanup of metastore references to deleted Iceberg metadata
This script directly manipulates the Hive metastore to remove orphaned table references
"""

import sys
import os
sys.path.insert(0, '/usr/local/spark_dev/work/payments_pipeline_ingestion/src')

import psycopg2
from payments_pipeline.utils.config import PipelineConfig

def cleanup_metastore():
    """Clean up orphaned table references in Hive metastore"""
    
    # Connect to PostgreSQL (Hive metastore)
    try:
        conn = psycopg2.connect(
            host="postgres",
            port="5432",
            database="metastore",
            user="hive",
            password="hive"
        )
        cursor = conn.cursor()
        
        print("🔍 Checking for orphaned table references...")
        
        # Find tables in payments_bronze database
        cursor.execute("""
            SELECT t.TBL_NAME, t.TBL_ID, s.LOCATION
            FROM "TBLS" t
            JOIN "DBS" d ON t.DB_ID = d.DB_ID
            JOIN "SDS" s ON t.SD_ID = s.SD_ID
            WHERE d."NAME" = 'payments_bronze'
        """)
        
        tables = cursor.fetchall()
        print(f"Found {len(tables)} tables in payments_bronze database:")
        
        for table_name, table_id, location in tables:
            print(f"  - {table_name} (ID: {table_id}, Location: {location})")
        
        if not tables:
            print("✅ No tables found in payments_bronze database")
            return
        
        # Remove table references
        print("\n🗑️ Removing orphaned table references...")
        
        for table_name, table_id, location in tables:
            print(f"  Removing {table_name} (ID: {table_id})...")
            
            # Delete from TBLS
            cursor.execute("DELETE FROM \"TBLS\" WHERE \"TBL_ID\" = %s", (table_id,))
            
            # Delete from SDS (storage descriptor)
            cursor.execute("DELETE FROM \"SDS\" WHERE \"SD_ID\" = %s", (table_id,))
            
            print(f"    ✅ Removed {table_name}")
        
        # Commit changes
        conn.commit()
        print("\n✅ Successfully cleaned up metastore references")
        
        # Verify cleanup
        cursor.execute("""
            SELECT COUNT(*) FROM "TBLS" t
            JOIN "DBS" d ON t.DB_ID = d.DB_ID
            WHERE d."NAME" = 'payments_bronze'
        """)
        
        remaining_count = cursor.fetchone()[0]
        print(f"📊 Remaining tables in payments_bronze: {remaining_count}")
        
        if remaining_count == 0:
            print("🎉 All orphaned references removed successfully!")
        else:
            print("⚠️ Some references may still exist")
        
    except Exception as e:
        print(f"❌ Error cleaning up metastore: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
    
    return True

if __name__ == "__main__":
    print("🧹 Force Cleanup of Hive Metastore")
    print("=" * 50)
    
    success = cleanup_metastore()
    
    if success:
        print("\n✅ Metastore cleanup completed successfully")
        print("You can now run the bronze ingestion again")
    else:
        print("\n❌ Metastore cleanup failed")
        sys.exit(1)
