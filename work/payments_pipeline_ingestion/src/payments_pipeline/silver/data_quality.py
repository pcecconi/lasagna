#!/usr/bin/env python3
"""
Data Quality Checks for Silver Layer

Comprehensive data quality validation for silver layer tables.
Ensures completeness, consistency, and referential integrity.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count as spark_count, sum as spark_sum, max as spark_max, min as spark_min

from ..utils.spark import get_spark_session
from ..utils.logging import setup_logging


class DataQualityChecker:
    """
    Data Quality Checker for Silver Layer
    
    Performs comprehensive data quality checks:
    - Completeness checks
    - Consistency checks
    - Referential integrity checks
    - Business rule validation
    """
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.logger = setup_logging(__name__)
        self.spark = spark_session or get_spark_session()
    
    def run_all_checks(self) -> Dict[str, any]:
        """
        Run all data quality checks
        
        Returns:
            Dict with check results and summary
        """
        self.logger.info("🔍 Starting comprehensive data quality checks...")
        
        results = {
            "timestamp": self.spark.sql("SELECT current_timestamp()").collect()[0][0],
            "checks": {},
            "summary": {
                "total_checks": 0,
                "passed_checks": 0,
                "failed_checks": 0,
                "warnings": 0
            }
        }
        
        # Run all checks
        checks = [
            ("merchant_completeness", self._check_merchant_completeness),
            ("transaction_completeness", self._check_transaction_completeness),
            ("referential_integrity", self._check_referential_integrity),
            ("foreign_key_integrity", self._check_foreign_key_integrity),
            ("data_consistency", self._check_data_consistency),
            ("scd_type2_integrity", self._check_scd_type2_integrity),
            ("duplicate_check", self._check_duplicates),
            ("business_rules", self._check_business_rules),
            ("data_types", self._check_data_types),
            ("null_values", self._check_null_values)
        ]
        
        for check_name, check_func in checks:
            try:
                self.logger.info(f"Running {check_name}...")
                result = check_func()
                results["checks"][check_name] = result
                results["summary"]["total_checks"] += 1
                
                if result["status"] == "PASS":
                    results["summary"]["passed_checks"] += 1
                elif result["status"] == "FAIL":
                    results["summary"]["failed_checks"] += 1
                elif result["status"] == "WARN":
                    results["summary"]["warnings"] += 1
                    
            except Exception as e:
                self.logger.error(f"Error running {check_name}: {e}")
                results["checks"][check_name] = {
                    "status": "ERROR",
                    "message": str(e),
                    "details": {}
                }
                results["summary"]["total_checks"] += 1
                results["summary"]["failed_checks"] += 1
        
        # Generate summary
        self._log_summary(results["summary"])
        
        return results
    
    def _check_merchant_completeness(self) -> Dict[str, any]:
        """Check if all bronze merchants appear in silver"""
        try:
            bronze_merchants = self.spark.table('spark_catalog.payments_bronze.merchants_raw')
            silver_merchants = self.spark.table('spark_catalog.payments_silver.dim_merchants')
            
            bronze_merchant_ids = set([row.merchant_id for row in bronze_merchants.select('merchant_id').distinct().collect()])
            silver_merchant_ids = set([row.merchant_id for row in silver_merchants.select('merchant_id').distinct().collect()])
            
            missing_merchants = bronze_merchant_ids - silver_merchant_ids
            extra_merchants = silver_merchant_ids - bronze_merchant_ids
            
            status = "PASS"
            message = "All bronze merchants present in silver"
            
            if missing_merchants:
                status = "FAIL"
                message = f"Missing {len(missing_merchants)} merchants in silver"
            elif extra_merchants:
                status = "WARN"
                message = f"Extra {len(extra_merchants)} merchants in silver"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "bronze_merchants": len(bronze_merchant_ids),
                    "silver_merchants": len(silver_merchant_ids),
                    "missing_merchants": len(missing_merchants),
                    "extra_merchants": len(extra_merchants),
                    "missing_merchant_ids": list(missing_merchants)[:10] if missing_merchants else [],
                    "extra_merchant_ids": list(extra_merchants)[:10] if extra_merchants else []
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking merchant completeness: {e}",
                "details": {}
            }
    
    def _check_transaction_completeness(self) -> Dict[str, any]:
        """Check if all bronze transactions appear in silver"""
        try:
            bronze_payments = self.spark.table('spark_catalog.payments_bronze.transactions_raw')
            silver_payments = self.spark.table('spark_catalog.payments_silver.fact_payments')
            
            bronze_payment_ids = set([row.payment_id for row in bronze_payments.select('payment_id').distinct().collect()])
            silver_payment_ids = set([row.payment_id for row in silver_payments.select('payment_id').distinct().collect()])
            
            missing_payments = bronze_payment_ids - silver_payment_ids
            extra_payments = silver_payment_ids - bronze_payment_ids
            
            status = "PASS"
            message = "All bronze payments present in silver"
            
            if missing_payments:
                status = "FAIL"
                message = f"Missing {len(missing_payments)} payments in silver"
            elif extra_payments:
                status = "WARN"
                message = f"Extra {len(extra_payments)} payments in silver"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "bronze_payments": len(bronze_payment_ids),
                    "silver_payments": len(silver_payment_ids),
                    "missing_payments": len(missing_payments),
                    "extra_payments": len(extra_payments),
                    "missing_payment_ids": list(missing_payments)[:10] if missing_payments else [],
                    "extra_payment_ids": list(extra_payments)[:10] if extra_payments else []
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking transaction completeness: {e}",
                "details": {}
            }
    
    def _check_referential_integrity(self) -> Dict[str, any]:
        """Check if all payments have merchant_sk"""
        try:
            silver_payments = self.spark.table('spark_catalog.payments_silver.fact_payments')
            
            payments_without_merchant_sk = silver_payments.filter('merchant_sk IS NULL').count()
            total_payments = silver_payments.count()
            
            status = "PASS" if payments_without_merchant_sk == 0 else "FAIL"
            message = "All payments have merchant_sk" if payments_without_merchant_sk == 0 else f"{payments_without_merchant_sk} payments missing merchant_sk"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "total_payments": total_payments,
                    "payments_without_merchant_sk": payments_without_merchant_sk,
                    "percentage_missing": (payments_without_merchant_sk / total_payments * 100) if total_payments > 0 else 0
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking referential integrity: {e}",
                "details": {}
            }
    
    def _check_foreign_key_integrity(self) -> Dict[str, any]:
        """Check if all merchant_sk values exist in dim_merchants"""
        try:
            silver_merchants = self.spark.table('spark_catalog.payments_silver.dim_merchants')
            silver_payments = self.spark.table('spark_catalog.payments_silver.fact_payments')
            
            silver_merchant_sks = set([row.merchant_sk for row in silver_merchants.select('merchant_sk').distinct().collect()])
            payment_merchant_sks = set([row.merchant_sk for row in silver_payments.select('merchant_sk').distinct().collect()])
            
            orphaned_merchant_sks = payment_merchant_sks - silver_merchant_sks
            
            status = "PASS" if len(orphaned_merchant_sks) == 0 else "FAIL"
            message = "All payment merchant_sk exist in dim_merchants" if len(orphaned_merchant_sks) == 0 else f"{len(orphaned_merchant_sks)} orphaned merchant_sk"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "unique_merchant_sks_in_dim": len(silver_merchant_sks),
                    "unique_merchant_sks_in_fact": len(payment_merchant_sks),
                    "orphaned_merchant_sks": len(orphaned_merchant_sks),
                    "orphaned_merchant_sk_list": list(orphaned_merchant_sks)[:10] if orphaned_merchant_sks else []
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking foreign key integrity: {e}",
                "details": {}
            }
    
    def _check_data_consistency(self) -> Dict[str, any]:
        """Check if payment amounts are consistent between bronze and silver"""
        try:
            bronze_payments = self.spark.table('spark_catalog.payments_bronze.transactions_raw')
            silver_payments = self.spark.table('spark_catalog.payments_silver.fact_payments')
            
            bronze_total_amount = bronze_payments.agg(spark_sum('payment_amount')).collect()[0][0]
            silver_total_amount = silver_payments.agg(spark_sum('payment_amount')).collect()[0][0]
            
            difference = abs(bronze_total_amount - silver_total_amount)
            percentage_diff = (difference / bronze_total_amount * 100) if bronze_total_amount > 0 else 0
            
            status = "PASS" if difference < 0.01 else "FAIL"
            message = "Payment amounts consistent" if difference < 0.01 else f"Payment amounts inconsistent by ${difference:,.2f}"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "bronze_total_amount": float(bronze_total_amount),
                    "silver_total_amount": float(silver_total_amount),
                    "difference": float(difference),
                    "percentage_difference": float(percentage_diff)
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking data consistency: {e}",
                "details": {}
            }
    
    def _check_scd_type2_integrity(self) -> Dict[str, any]:
        """Check SCD Type 2 integrity"""
        try:
            scd_check = self.spark.sql('''
            SELECT 
                merchant_id,
                COUNT(*) as version_count,
                SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) as current_count
            FROM spark_catalog.payments_silver.dim_merchants
            GROUP BY merchant_id
            HAVING COUNT(*) > 1 OR SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) != 1
            ''')
            
            scd_issues = scd_check.count()
            
            status = "PASS" if scd_issues == 0 else "FAIL"
            message = "SCD Type 2 integrity maintained" if scd_issues == 0 else f"{scd_issues} merchants with SCD issues"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "merchants_with_scd_issues": scd_issues,
                    "scd_issues": scd_check.collect() if scd_issues > 0 else []
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking SCD Type 2 integrity: {e}",
                "details": {}
            }
    
    def _check_duplicates(self) -> Dict[str, any]:
        """Check for duplicate payment_ids"""
        try:
            duplicate_payments = self.spark.sql('''
            SELECT payment_id, COUNT(*) as count
            FROM spark_catalog.payments_silver.fact_payments
            GROUP BY payment_id
            HAVING COUNT(*) > 1
            ''')
            
            duplicate_count = duplicate_payments.count()
            
            status = "PASS" if duplicate_count == 0 else "FAIL"
            message = "No duplicate payment_ids" if duplicate_count == 0 else f"{duplicate_count} duplicate payment_ids"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "duplicate_count": duplicate_count,
                    "duplicates": duplicate_payments.collect() if duplicate_count > 0 else []
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking duplicates: {e}",
                "details": {}
            }
    
    def _check_business_rules(self) -> Dict[str, any]:
        """Check business rule validations"""
        try:
            silver_payments = self.spark.table('spark_catalog.payments_silver.fact_payments')
            silver_merchants = self.spark.table('spark_catalog.payments_silver.dim_merchants')
            
            issues = []
            
            # Check payment amount range
            invalid_amounts = silver_payments.filter(col("payment_amount") <= 0).count()
            if invalid_amounts > 0:
                issues.append(f"{invalid_amounts} payments with invalid amounts")
            
            # Check MDR rate range
            invalid_mdr_rates = silver_merchants.filter(
                (col("mdr_rate") < 0.01) | (col("mdr_rate") > 0.10)
            ).count()
            if invalid_mdr_rates > 0:
                issues.append(f"{invalid_mdr_rates} merchants with invalid MDR rates")
            
            # Check payment status values
            invalid_statuses = silver_payments.filter(
                ~col("payment_status").isin(["approved", "declined", "cancelled"])
            ).count()
            if invalid_statuses > 0:
                issues.append(f"{invalid_statuses} payments with invalid status")
            
            status = "PASS" if len(issues) == 0 else "FAIL"
            message = "All business rules satisfied" if len(issues) == 0 else f"Business rule violations: {', '.join(issues)}"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "invalid_amounts": invalid_amounts,
                    "invalid_mdr_rates": invalid_mdr_rates,
                    "invalid_statuses": invalid_statuses,
                    "issues": issues
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking business rules: {e}",
                "details": {}
            }
    
    def _check_data_types(self) -> Dict[str, any]:
        """Check data type consistency"""
        try:
            silver_payments = self.spark.table('spark_catalog.payments_silver.fact_payments')
            silver_merchants = self.spark.table('spark_catalog.payments_silver.dim_merchants')
            
            # Check for type mismatches
            issues = []
            
            # Check if payment_amount is numeric
            try:
                silver_payments.select("payment_amount").agg(spark_sum("payment_amount")).collect()
            except:
                issues.append("payment_amount contains non-numeric values")
            
            # Check if merchant_sk is numeric
            try:
                silver_payments.select("merchant_sk").agg(spark_max("merchant_sk")).collect()
            except:
                issues.append("merchant_sk contains non-numeric values")
            
            status = "PASS" if len(issues) == 0 else "FAIL"
            message = "All data types consistent" if len(issues) == 0 else f"Data type issues: {', '.join(issues)}"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "issues": issues,
                    "payment_schema": [str(field) for field in silver_payments.schema.fields],
                    "merchant_schema": [str(field) for field in silver_merchants.schema.fields]
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking data types: {e}",
                "details": {}
            }
    
    def _check_null_values(self) -> Dict[str, any]:
        """Check for unexpected null values"""
        try:
            silver_payments = self.spark.table('spark_catalog.payments_silver.fact_payments')
            silver_merchants = self.spark.table('spark_catalog.payments_silver.dim_merchants')
            
            issues = []
            
            # Check critical fields for nulls
            critical_payment_fields = ["payment_id", "merchant_id", "payment_amount", "merchant_sk"]
            for field in critical_payment_fields:
                null_count = silver_payments.filter(col(field).isNull()).count()
                if null_count > 0:
                    issues.append(f"{null_count} null values in payments.{field}")
            
            critical_merchant_fields = ["merchant_id", "merchant_name", "mdr_rate"]
            for field in critical_merchant_fields:
                null_count = silver_merchants.filter(col(field).isNull()).count()
                if null_count > 0:
                    issues.append(f"{null_count} null values in merchants.{field}")
            
            status = "PASS" if len(issues) == 0 else "WARN"
            message = "No unexpected null values" if len(issues) == 0 else f"Null value issues: {', '.join(issues)}"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "issues": issues
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking null values: {e}",
                "details": {}
            }
    
    def _log_summary(self, summary: Dict[str, any]):
        """Log data quality check summary"""
        self.logger.info("📊 Data Quality Check Summary:")
        self.logger.info(f"  Total checks: {summary['total_checks']}")
        self.logger.info(f"  Passed: {summary['passed_checks']}")
        self.logger.info(f"  Failed: {summary['failed_checks']}")
        self.logger.info(f"  Warnings: {summary['warnings']}")
        
        if summary['failed_checks'] > 0:
            self.logger.error("❌ Data quality checks failed!")
        elif summary['warnings'] > 0:
            self.logger.warning("⚠️ Data quality checks passed with warnings")
        else:
            self.logger.info("✅ All data quality checks passed!")
    
    def generate_report(self, results: Dict[str, any]) -> str:
        """Generate a human-readable data quality report"""
        report = []
        report.append("=" * 80)
        report.append("DATA QUALITY REPORT")
        report.append("=" * 80)
        report.append(f"Timestamp: {results['timestamp']}")
        report.append("")
        
        # Summary
        summary = results['summary']
        report.append("SUMMARY:")
        report.append(f"  Total checks: {summary['total_checks']}")
        report.append(f"  Passed: {summary['passed_checks']}")
        report.append(f"  Failed: {summary['failed_checks']}")
        report.append(f"  Warnings: {summary['warnings']}")
        report.append("")
        
        # Individual checks
        report.append("DETAILED RESULTS:")
        report.append("-" * 40)
        
        for check_name, check_result in results['checks'].items():
            status_icon = "✅" if check_result['status'] == "PASS" else "❌" if check_result['status'] == "FAIL" else "⚠️"
            report.append(f"{status_icon} {check_name.upper()}: {check_result['status']}")
            report.append(f"   {check_result['message']}")
            
            if check_result['details']:
                for key, value in check_result['details'].items():
                    if isinstance(value, list) and len(value) > 5:
                        report.append(f"   {key}: {len(value)} items (showing first 5)")
                    else:
                        report.append(f"   {key}: {value}")
            report.append("")
        
        return "\n".join(report)


def main():
    """Main entry point for data quality checker"""
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description='Data Quality Checker for Silver Layer')
    parser.add_argument('--output', help='Output file for results (JSON)')
    parser.add_argument('--report', help='Output file for human-readable report')
    
    args = parser.parse_args()
    
    # Run data quality checks
    checker = DataQualityChecker()
    results = checker.run_all_checks()
    
    # Save results
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Results saved to {args.output}")
    
    # Generate and save report
    if args.report:
        report = checker.generate_report(results)
        with open(args.report, 'w') as f:
            f.write(report)
        print(f"Report saved to {args.report}")
    
    # Print summary
    summary = results['summary']
    if summary['failed_checks'] > 0:
        print("❌ Data quality checks failed!")
        exit(1)
    elif summary['warnings'] > 0:
        print("⚠️ Data quality checks passed with warnings")
    else:
        print("✅ All data quality checks passed!")


if __name__ == "__main__":
    main()
