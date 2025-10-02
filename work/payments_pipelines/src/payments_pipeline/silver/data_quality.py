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
from ..utils.config import PipelineConfig
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
        self.config = PipelineConfig()
        
        # Test Spark session connectivity
        self._test_spark_connectivity()
    
    def _test_spark_connectivity(self):
        """Test if Spark session is working properly"""
        try:
            # Simple test query
            test_result = self.spark.sql("SELECT 1 as test").collect()[0]['test']
            if test_result != 1:
                raise Exception("Spark session test failed")
            self.logger.info("✅ Spark session connectivity verified")
        except Exception as e:
            self.logger.error(f"❌ Spark session connectivity test failed: {e}")
            raise Exception(f"Spark session is not available: {e}")
    
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
            ("null_values", self._check_null_values),
            ("v2_schema_validation", self._check_v2_schema_validation)
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
                # Check if it's a connectivity issue
                if "Py4JNetworkError" in str(e) or "Connection refused" in str(e):
                    self.logger.error("⚠️ Spark connectivity issue detected. Skipping remaining checks.")
                    results["checks"][check_name] = {
                        "status": "ERROR",
                        "message": f"Spark connectivity issue: {str(e)}",
                        "details": {"error_type": "connectivity"}
                    }
                    results["summary"]["total_checks"] += 1
                    results["summary"]["failed_checks"] += 1
                    break  # Stop running more checks if connectivity is lost
                else:
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
            # Use SQL to avoid large collect() operations
            bronze_count = self.spark.sql(f"""
                SELECT COUNT(DISTINCT merchant_id) as count 
                FROM {self.config.iceberg_catalog}.{self.config.bronze_namespace}.merchants_raw
            """).collect()[0]['count']
            
            silver_count = self.spark.sql(f"""
                SELECT COUNT(DISTINCT merchant_id) as count 
                FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants
            """).collect()[0]['count']
            
            # Check for missing merchants using SQL
            missing_merchants_df = self.spark.sql(f"""
                SELECT b.merchant_id
                FROM {self.config.iceberg_catalog}.{self.config.bronze_namespace}.merchants_raw b
                LEFT JOIN {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants s
                ON b.merchant_id = s.merchant_id
                WHERE s.merchant_id IS NULL
                LIMIT 10
            """)
            missing_merchants = [row.merchant_id for row in missing_merchants_df.collect()]
            
            # Check for extra merchants using SQL
            extra_merchants_df = self.spark.sql(f"""
                SELECT s.merchant_id
                FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants s
                LEFT JOIN {self.config.iceberg_catalog}.{self.config.bronze_namespace}.merchants_raw b
                ON s.merchant_id = b.merchant_id
                WHERE b.merchant_id IS NULL
                LIMIT 10
            """)
            extra_merchants = [row.merchant_id for row in extra_merchants_df.collect()]
            
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
                    "bronze_merchants": bronze_count,
                    "silver_merchants": silver_count,
                    "missing_merchants": len(missing_merchants),
                    "extra_merchants": len(extra_merchants),
                    "missing_merchant_ids": missing_merchants,
                    "extra_merchant_ids": extra_merchants
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
            # Use SQL to avoid large collect() operations
            bronze_count = self.spark.sql(f"""
                SELECT COUNT(DISTINCT payment_id) as count 
                FROM {self.config.iceberg_catalog}.{self.config.bronze_namespace}.transactions_raw
            """).collect()[0]['count']
            
            silver_count = self.spark.sql(f"""
                SELECT COUNT(DISTINCT payment_id) as count 
                FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments
            """).collect()[0]['count']
            
            # Check for missing payments using SQL
            missing_payments_df = self.spark.sql(f"""
                SELECT b.payment_id
                FROM {self.config.iceberg_catalog}.{self.config.bronze_namespace}.transactions_raw b
                LEFT JOIN {self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments s
                ON b.payment_id = s.payment_id
                WHERE s.payment_id IS NULL
                LIMIT 10
            """)
            missing_payments = [row.payment_id for row in missing_payments_df.collect()]
            
            # Check for extra payments using SQL
            extra_payments_df = self.spark.sql(f"""
                SELECT s.payment_id
                FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments s
                LEFT JOIN {self.config.iceberg_catalog}.{self.config.bronze_namespace}.transactions_raw b
                ON s.payment_id = b.payment_id
                WHERE b.payment_id IS NULL
                LIMIT 10
            """)
            extra_payments = [row.payment_id for row in extra_payments_df.collect()]
            
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
                    "bronze_payments": bronze_count,
                    "silver_payments": silver_count,
                    "missing_payments": len(missing_payments),
                    "extra_payments": len(extra_payments),
                    "missing_payment_ids": missing_payments,
                    "extra_payment_ids": extra_payments
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
            silver_payments = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments')
            
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
            # Use SQL to avoid large collect() operations
            dim_merchant_sk_count = self.spark.sql(f"""
                SELECT COUNT(DISTINCT merchant_sk) as count 
                FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants
            """).collect()[0]['count']
            
            fact_merchant_sk_count = self.spark.sql(f"""
                SELECT COUNT(DISTINCT merchant_sk) as count 
                FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments
            """).collect()[0]['count']
            
            # Check for orphaned merchant_sk using SQL
            orphaned_merchant_sks_df = self.spark.sql(f"""
                SELECT DISTINCT f.merchant_sk
                FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments f
                LEFT JOIN {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants d
                ON f.merchant_sk = d.merchant_sk
                WHERE d.merchant_sk IS NULL AND f.merchant_sk IS NOT NULL
                LIMIT 10
            """)
            orphaned_merchant_sks = [row.merchant_sk for row in orphaned_merchant_sks_df.collect()]
            
            status = "PASS" if len(orphaned_merchant_sks) == 0 else "FAIL"
            message = "All payment merchant_sk exist in dim_merchants" if len(orphaned_merchant_sks) == 0 else f"{len(orphaned_merchant_sks)} orphaned merchant_sk"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "unique_merchant_sks_in_dim": dim_merchant_sk_count,
                    "unique_merchant_sks_in_fact": fact_merchant_sk_count,
                    "orphaned_merchant_sks": len(orphaned_merchant_sks),
                    "orphaned_merchant_sk_list": orphaned_merchant_sks
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
            bronze_payments = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.bronze_namespace}.transactions_raw')
            silver_payments = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments')
            
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
            # Check 1: Merchants with 0 or more than 1 current record
            current_record_issues = self.spark.sql(f'''
            SELECT 
                merchant_id,
                COUNT(*) as version_count,
                SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) as current_count
            FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants
            GROUP BY merchant_id
            HAVING SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) != 1
            ''')
            
            current_record_count = current_record_issues.count()
            
            # Check 2: Merchants with overlapping effective/expiry date ranges (for non-current records)
            overlapping_dates = self.spark.sql(f'''
            SELECT DISTINCT a.merchant_id
            FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants a
            JOIN {self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants b
            ON a.merchant_id = b.merchant_id 
            AND a.merchant_sk != b.merchant_sk
            AND a.is_current = false 
            AND b.is_current = false
            WHERE (a.effective_date <= b.effective_date AND a.expiry_date > b.effective_date)
               OR (b.effective_date <= a.effective_date AND b.expiry_date > a.effective_date)
            ''')
            
            overlapping_count = overlapping_dates.count()
            
            total_issues = current_record_count + overlapping_count
            
            # Collect examples for debugging
            current_examples = current_record_issues.limit(3).collect() if current_record_count > 0 else []
            overlapping_examples = overlapping_dates.limit(3).collect() if overlapping_count > 0 else []
            
            status = "PASS" if total_issues == 0 else "FAIL"
            message = "SCD Type 2 integrity maintained" if total_issues == 0 else f"{total_issues} merchants with SCD issues"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "current_record_issues": current_record_count,
                    "overlapping_date_issues": overlapping_count,
                    "total_scd_issues": total_issues,
                    "current_record_examples": [{"merchant_id": row.merchant_id, "version_count": row.version_count, "current_count": row.current_count} for row in current_examples],
                    "overlapping_date_examples": [{"merchant_id": row.merchant_id} for row in overlapping_examples]
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
            duplicate_payments = self.spark.sql(f'''
            SELECT payment_id, COUNT(*) as count
            FROM {self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments
            GROUP BY payment_id
            HAVING COUNT(*) > 1
            ''')
            
            duplicate_count = duplicate_payments.count()
            
            # Only collect a few examples to avoid large data transfer
            duplicate_examples = duplicate_payments.limit(5).collect() if duplicate_count > 0 else []
            
            status = "PASS" if duplicate_count == 0 else "FAIL"
            message = "No duplicate payment_ids" if duplicate_count == 0 else f"{duplicate_count} duplicate payment_ids"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "duplicate_count": duplicate_count,
                    "duplicates": [{"payment_id": row.payment_id, "count": row.count} for row in duplicate_examples]
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
            silver_payments = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments')
            silver_merchants = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants')
            
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
            silver_payments = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments')
            silver_merchants = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants')
            
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
            silver_payments = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments')
            silver_merchants = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants')
            
            issues = []
            
            # Check critical fields for nulls
            critical_payment_fields = ["payment_id", "merchant_id", "payment_amount", "merchant_sk"]
            for field in critical_payment_fields:
                null_count = silver_payments.filter(col(field).isNull()).count()
                if null_count > 0:
                    issues.append(f"{null_count} null values in payments.{field}")
            
            # Check V2 payment fields (these can be null, but we'll track them)
            v2_payment_fields = ["card_profile_id", "card_bin"]
            for field in v2_payment_fields:
                null_count = silver_payments.filter(col(field).isNull()).count()
                total_count = silver_payments.count()
                null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
                if null_percentage > 50:  # Warn if more than 50% are null
                    issues.append(f"{null_count} ({null_percentage:.1f}%) null values in payments.{field}")
            
            critical_merchant_fields = ["merchant_id", "merchant_name", "mdr_rate"]
            for field in critical_merchant_fields:
                null_count = silver_merchants.filter(col(field).isNull()).count()
                if null_count > 0:
                    issues.append(f"{null_count} null values in merchants.{field}")
            
            # Check V2 merchant fields
            v2_merchant_fields = ["version", "change_type"]
            for field in v2_merchant_fields:
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
    
    def _check_v2_schema_validation(self) -> Dict[str, any]:
        """Check V2 schema validation for new columns"""
        try:
            silver_merchants = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.silver_namespace}.dim_merchants')
            silver_payments = self.spark.table(f'{self.config.iceberg_catalog}.{self.config.silver_namespace}.fact_payments')
            
            issues = []
            
            # Check V2 columns exist in dim_merchants
            merchant_columns = [field.name for field in silver_merchants.schema.fields]
            required_merchant_v2_columns = ['version', 'change_type']
            for col_name in required_merchant_v2_columns:
                if col_name not in merchant_columns:
                    issues.append(f"Missing V2 column 'version' in dim_merchants")
            
            # Check V2 columns exist in fact_payments
            payment_columns = [field.name for field in silver_payments.schema.fields]
            required_payment_v2_columns = ['card_profile_id', 'card_bin']
            for col_name in required_payment_v2_columns:
                if col_name not in payment_columns:
                    issues.append(f"Missing V2 column '{col_name}' in fact_payments")
            
            # Check version column values are positive integers
            if 'version' in merchant_columns:
                invalid_versions = silver_merchants.filter(
                    (col("version").isNull()) | (col("version") <= 0)
                ).count()
                if invalid_versions > 0:
                    issues.append(f"{invalid_versions} merchants with invalid version values")
            
            # Check change_type column has valid values
            if 'change_type' in merchant_columns:
                valid_change_types = ['initial', 'attribute_change', 'missing_merchant']
                invalid_change_types = silver_merchants.filter(
                    ~col("change_type").isin(valid_change_types)
                ).count()
                if invalid_change_types > 0:
                    issues.append(f"{invalid_change_types} merchants with invalid change_type values")
            
            # Check card_profile_id format (should be like CARD123456)
            if 'card_profile_id' in payment_columns:
                invalid_card_profiles = silver_payments.filter(
                    (col("card_profile_id").isNotNull()) & 
                    (~col("card_profile_id").rlike("^CARD[0-9]{6}$"))
                ).count()
                if invalid_card_profiles > 0:
                    issues.append(f"{invalid_card_profiles} payments with invalid card_profile_id format")
            
            # Check card_bin format (should be 6 digits)
            if 'card_bin' in payment_columns:
                invalid_card_bins = silver_payments.filter(
                    (col("card_bin").isNotNull()) & 
                    (~col("card_bin").rlike("^[0-9]{6}$"))
                ).count()
                if invalid_card_bins > 0:
                    issues.append(f"{invalid_card_bins} payments with invalid card_bin format")
            
            status = "PASS" if len(issues) == 0 else "FAIL"
            message = "V2 schema validation passed" if len(issues) == 0 else f"V2 schema issues: {', '.join(issues)}"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "issues": issues,
                    "merchant_v2_columns": [col for col in required_merchant_v2_columns if col in merchant_columns],
                    "payment_v2_columns": [col for col in required_payment_v2_columns if col in payment_columns],
                    "total_merchants": silver_merchants.count(),
                    "total_payments": silver_payments.count()
                }
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Error checking V2 schema validation: {e}",
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

