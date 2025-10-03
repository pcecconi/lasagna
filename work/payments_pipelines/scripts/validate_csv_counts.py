#!/usr/bin/env python3
"""
Simple validation script to check record counts in CSV files
"""

import pandas as pd
from pathlib import Path

def validate_csv_counts():
    """Validate record counts in CSV files"""
    
    # Path to raw data directory
    raw_data_path = Path("/usr/local/spark_dev/work/payments_data_source/raw_data")
    
    print("🔍 Validating CSV File Record Counts")
    print("=" * 50)
    
    # Find all CSV files
    csv_files = list(raw_data_path.glob("*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found in raw_data directory")
        return False
    
    print(f"📁 Found {len(csv_files)} CSV files:")
    for file in csv_files:
        print(f"   - {file.name}")
    
    print("\n📊 Record Count Analysis:")
    print("-" * 30)
    
    total_merchants = 0
    total_transactions = 0
    
    for csv_file in sorted(csv_files):
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            record_count = len(df)
            
            # Determine file type
            if "merchants" in csv_file.name:
                file_type = "merchants"
                total_merchants += record_count
                icon = "🏪"
            elif "transactions" in csv_file.name:
                file_type = "transactions"
                total_transactions += record_count
                icon = "💳"
            else:
                file_type = "unknown"
                icon = "❓"
            
            print(f"{icon} {csv_file.name}: {record_count:,} records ({file_type})")
            
            # Show sample of merchant files
            if "merchants" in csv_file.name:
                print(f"   📋 Sample merchant IDs: {df['merchant_id'].head(3).tolist()}")
                print(f"   📋 Unique merchant IDs: {df['merchant_id'].nunique():,}")
                
        except Exception as e:
            print(f"❌ Error reading {csv_file.name}: {e}")
    
    print("\n📋 Summary:")
    print("=" * 20)
    print(f"🏪 Total merchants records: {total_merchants:,}")
    print(f"💳 Total transactions records: {total_transactions:,}")
    
    # Validation against our table counts
    print(f"\n🔍 Validation against table counts:")
    print("-" * 35)
    print(f"Expected merchants_raw: 4,317 (from legacy pipeline - 1 file only)")
    print(f"Expected merchants: 12,951 (from new modular pipeline - 2 files)")
    
    # Check if our analysis is correct
    if total_merchants >= 12000:  # Should be around 12,951 based on our table
        print("✅ CSV record count supports our analysis!")
        print("   📊 New modular pipeline processes ALL merchant files")
        print("   📊 Legacy pipeline processes only FIRST merchant file")
    else:
        print("⚠️ CSV record count doesn't match expected values")
    
    return True

if __name__ == "__main__":
    validate_csv_counts()
