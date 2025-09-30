#!/usr/bin/env python3
"""
Silver Layer Ingestion

Main entry point for silver layer processing with atomic updates.
Orchestrates the complete silver layer pipeline.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, count as spark_count, max as spark_max, min as spark_min

from .atomic_updates import AtomicSilverUpdater
from .data_quality import DataQualityChecker
from ..utils.spark import get_spark_session
from ..utils.config import PipelineConfig
from ..utils.logging import setup_logging


class SilverIngestionJob:
    """
    Silver Layer Ingestion Job
    
    Orchestrates the complete silver layer pipeline:
    - Atomic updates with SCD Type 2 logic
    - Window-based processing
    - Data quality validation
    - Error handling and rollback
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None, spark_session=None):
        self.config = config or PipelineConfig()
        self.logger = setup_logging(__name__)
        
        # Use provided Spark session or get from utils
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = get_spark_session()
        
        # Initialize atomic updater and data quality checker
        self.atomic_updater = AtomicSilverUpdater(config, spark_session)
        self.data_quality_checker = DataQualityChecker(spark_session)
    
    def run_complete_silver_pipeline(self, 
                                   bronze_merchants_df: DataFrame,
                                   bronze_payments_df: DataFrame,
                                   processing_window: str = "daily") -> bool:
        """
        Run complete silver layer pipeline
        
        Args:
            bronze_merchants_df: Merchant data from bronze layer
            bronze_payments_df: Payment data from bronze layer
            processing_window: Processing window ("daily", "weekly", "monthly")
            
        Returns:
            bool: True if pipeline succeeded, False otherwise
        """
        try:
            self.logger.info(f"🚀 Starting silver layer pipeline ({processing_window})")
            
            # Step 1: Update merchants (always full comparison)
            self.logger.info("📊 Processing merchants...")
            if not self.atomic_updater.atomic_update_merchants(bronze_merchants_df):
                self.logger.error("❌ Merchant processing failed")
                return False
            
            # Step 2: Update payments with window-based processing
            self.logger.info("💳 Processing payments...")
            date_range = self._get_processing_window(processing_window)
            if not self.atomic_updater.atomic_update_payments(bronze_payments_df, date_range):
                self.logger.error("❌ Payment processing failed")
                return False
            
            # Step 3: Run comprehensive data quality checks
            self.logger.info("🔍 Running data quality checks...")
            dq_results = self.data_quality_checker.run_all_checks()
            
            # Check if any critical checks failed
            failed_checks = [name for name, result in dq_results['checks'].items() 
                           if result['status'] == 'FAIL']
            
            if failed_checks:
                self.logger.error(f"❌ Data quality checks failed: {failed_checks}")
                self.logger.error("Data quality report:")
                print(self.data_quality_checker.generate_report(dq_results))
                return False
            
            # Log warnings if any
            warning_checks = [name for name, result in dq_results['checks'].items() 
                            if result['status'] == 'WARN']
            if warning_checks:
                self.logger.warning(f"⚠️ Data quality warnings: {warning_checks}")
            
            self.logger.info("🎉 Silver layer pipeline completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Silver layer pipeline failed: {e}")
            return False
    
    def _get_processing_window(self, window_type: str) -> Tuple[date, date]:
        """Get processing window dates based on window type"""
        current_date = date.today()
        
        if window_type == "daily":
            # Process last 7 days
            start_date = current_date - timedelta(days=7)
            end_date = current_date
        elif window_type == "weekly":
            # Process last 30 days
            start_date = current_date - timedelta(days=30)
            end_date = current_date
        elif window_type == "monthly":
            # Process last 90 days
            start_date = current_date - timedelta(days=90)
            end_date = current_date
        else:
            raise ValueError(f"Unknown processing window: {window_type}")
        
        return start_date, end_date
    
    def get_data_quality_results(self) -> Dict[str, any]:
        """Get latest data quality check results"""
        return self.data_quality_checker.run_all_checks()
    
    def get_silver_layer_stats(self) -> Dict[str, any]:
        """Get silver layer statistics"""
        try:
            stats = {}
            
            # Merchants stats
            merchants_df = self.spark.table("spark_catalog.payments_silver.dim_merchants")
            stats["merchants"] = {
                "total_count": merchants_df.count(),
                "current_count": merchants_df.filter(col("is_current") == True).count(),
                "historical_count": merchants_df.filter(col("is_current") == False).count()
            }
            
            # Payments stats
            payments_df = self.spark.table("spark_catalog.payments_silver.fact_payments")
            payments_agg = payments_df.agg(
                spark_count("*").alias("total_count"),
                spark_max("payment_amount").alias("max_amount"),
                spark_min("payment_amount").alias("min_amount")
            ).collect()[0]
            
            stats["payments"] = {
                "total_count": payments_agg["total_count"],
                "max_amount": payments_agg["max_amount"],
                "min_amount": payments_agg["min_amount"]
            }
            
            # Date range
            date_stats = payments_df.agg(
                spark_min("payment_date").alias("min_date"),
                spark_max("payment_date").alias("max_date")
            ).collect()[0]
            stats["date_range"] = {
                "start_date": date_stats["min_date"],
                "end_date": date_stats["max_date"]
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"❌ Error getting stats: {e}")
            return {}
    
    def run_incremental_update(self, 
                             bronze_merchants_df: DataFrame,
                             bronze_payments_df: DataFrame) -> bool:
        """Run incremental update (daily processing)"""
        return self.run_complete_silver_pipeline(
            bronze_merchants_df, 
            bronze_payments_df, 
            "daily"
        )
    
    def run_full_reprocess(self, 
                          bronze_merchants_df: DataFrame,
                          bronze_payments_df: DataFrame) -> bool:
        """Run full reprocessing (monthly processing)"""
        return self.run_complete_silver_pipeline(
            bronze_merchants_df, 
            bronze_payments_df, 
            "monthly"
        )


def main():
    """Main entry point for silver ingestion job"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Silver Layer Ingestion Job')
    parser.add_argument('--bronze-merchants', help='Path to bronze merchants data')
    parser.add_argument('--bronze-payments', help='Path to bronze payments data')
    parser.add_argument('--processing-window', choices=['daily', 'weekly', 'monthly'], 
                       default='daily', help='Processing window')
    parser.add_argument('--stats', action='store_true', help='Show silver layer statistics')
    parser.add_argument('--data-quality', action='store_true', help='Run data quality checks only')
    parser.add_argument('--dq-output', help='Output file for data quality results (JSON)')
    parser.add_argument('--dq-report', help='Output file for data quality report')
    
    args = parser.parse_args()
    
    # Initialize job
    job = SilverIngestionJob()
    
    if args.stats:
        stats = job.get_silver_layer_stats()
        print("📊 Silver Layer Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        return
    
    if args.data_quality:
        # Run data quality checks only
        dq_results = job.get_data_quality_results()
        
        # Save results if requested
        if args.dq_output:
            import json
            with open(args.dq_output, 'w') as f:
                json.dump(dq_results, f, indent=2, default=str)
            print(f"Data quality results saved to {args.dq_output}")
        
        # Generate and save report if requested
        if args.dq_report:
            report = job.data_quality_checker.generate_report(dq_results)
            with open(args.dq_report, 'w') as f:
                f.write(report)
            print(f"Data quality report saved to {args.dq_report}")
        
        # Print summary
        summary = dq_results['summary']
        if summary['failed_checks'] > 0:
            print("❌ Data quality checks failed!")
            print(job.data_quality_checker.generate_report(dq_results))
            sys.exit(1)
        elif summary['warnings'] > 0:
            print("⚠️ Data quality checks passed with warnings")
        else:
            print("✅ All data quality checks passed!")
        return
    
    if not args.bronze_merchants or not args.bronze_payments:
        print("❌ --bronze-merchants and --bronze-payments are required")
        return
    
    # Read bronze data
    bronze_merchants_df = job.spark.read.table(args.bronze_merchants)
    bronze_payments_df = job.spark.read.table(args.bronze_payments)
    
    # Run pipeline
    success = job.run_complete_silver_pipeline(
        bronze_merchants_df,
        bronze_payments_df,
        args.processing_window
    )
    
    if success:
        job.logger.info("🎉 Silver layer ingestion completed successfully")
    else:
        job.logger.error("❌ Silver layer ingestion failed")


if __name__ == "__main__":
    main()
