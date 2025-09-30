#!/usr/bin/env python3
"""
System Optimization Script for Lasagna

This script helps optimize the Lasagna system for low-memory environments.
It provides options to switch between normal and optimized configurations.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path


def backup_current_config():
    """Backup current docker-compose.yml"""
    if Path("docker-compose.yml").exists():
        shutil.copy("docker-compose.yml", "docker-compose.yml.backup")
        print("✅ Backed up current docker-compose.yml")
    else:
        print("⚠️ No existing docker-compose.yml found")


def apply_optimized_config():
    """Apply optimized configuration"""
    if Path("docker-compose.optimized.yml").exists():
        shutil.copy("docker-compose.optimized.yml", "docker-compose.yml")
        print("✅ Applied optimized configuration")
    else:
        print("❌ Optimized configuration file not found")


def restore_original_config():
    """Restore original configuration"""
    if Path("docker-compose.yml.backup").exists():
        shutil.copy("docker-compose.yml.backup", "docker-compose.yml")
        print("✅ Restored original configuration")
    else:
        print("⚠️ No backup found")


def update_spark_utils():
    """Update Spark utils to use optimized configuration"""
    spark_utils_path = Path("work/payments_pipeline_ingestion/src/payments_pipeline/utils/spark.py")
    spark_optimized_path = Path("work/payments_pipeline_ingestion/src/payments_pipeline/utils/spark_optimized.py")
    
    if spark_optimized_path.exists():
        # Backup original
        if spark_utils_path.exists():
            shutil.copy(spark_utils_path, str(spark_utils_path) + ".backup")
        
        # Replace with optimized version
        shutil.copy(spark_optimized_path, spark_utils_path)
        print("✅ Updated Spark utils to use optimized configuration")
    else:
        print("❌ Optimized Spark utils not found")


def restore_spark_utils():
    """Restore original Spark utils"""
    spark_utils_path = Path("work/payments_pipeline_ingestion/src/payments_pipeline/utils/spark.py")
    spark_backup_path = Path("work/payments_pipeline_ingestion/src/payments_pipeline/utils/spark.py.backup")
    
    if spark_backup_path.exists():
        shutil.copy(spark_backup_path, spark_utils_path)
        print("✅ Restored original Spark utils")
    else:
        print("⚠️ No Spark utils backup found")


def restart_containers():
    """Restart Docker containers with new configuration"""
    print("🔄 Restarting Docker containers...")
    try:
        # Stop all containers
        subprocess.run(["docker", "compose", "down"], check=True)
        print("✅ Stopped all containers")
        
        # Start with new configuration
        subprocess.run(["docker", "compose", "up", "-d"], check=True)
        print("✅ Started containers with new configuration")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error restarting containers: {e}")
        return False
    
    return True


def check_system_status():
    """Check current system status"""
    print("\n📊 Current System Status:")
    print("=" * 50)
    
    # Check memory
    try:
        result = subprocess.run(["free", "-h"], capture_output=True, text=True)
        print("Memory Usage:")
        print(result.stdout)
    except:
        print("Could not check memory usage")
    
    # Check Docker stats
    try:
        result = subprocess.run(["docker", "stats", "--no-stream"], capture_output=True, text=True)
        print("Docker Container Stats:")
        print(result.stdout)
    except:
        print("Could not check Docker stats")


def main():
    """Main function"""
    print("🚀 Lasagna System Optimizer")
    print("=" * 50)
    
    if len(sys.argv) < 2:
        print("Usage: python optimize_system.py [optimize|restore|status]")
        print("\nCommands:")
        print("  optimize  - Apply optimized configuration for low-memory systems")
        print("  restore   - Restore original configuration")
        print("  status    - Check current system status")
        return
    
    command = sys.argv[1].lower()
    
    if command == "optimize":
        print("🔧 Applying optimized configuration...")
        backup_current_config()
        apply_optimized_config()
        update_spark_utils()
        
        print("\n⚠️ IMPORTANT: You need to restart the containers manually:")
        print("   docker compose down")
        print("   docker compose up -d")
        print("\nThis will:")
        print("- Reduce memory allocation to ~7GB total (from ~10GB)")
        print("- Use only 1 Spark worker instead of 2")
        print("- Apply conservative Spark memory settings")
        print("- Optimize for your 15GB system")
        
    elif command == "restore":
        print("🔄 Restoring original configuration...")
        restore_original_config()
        restore_spark_utils()
        
        print("\n⚠️ IMPORTANT: You need to restart the containers manually:")
        print("   docker compose down")
        print("   docker compose up -d")
        
    elif command == "status":
        check_system_status()
        
    else:
        print(f"❌ Unknown command: {command}")
        print("Available commands: optimize, restore, status")


if __name__ == "__main__":
    main()
