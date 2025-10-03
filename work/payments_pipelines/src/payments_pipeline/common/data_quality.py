#!/usr/bin/env python3
"""
Data Quality Framework

Provides a unified data quality validation framework for all pipeline layers.
"""

import logging
from typing import Dict, List, Any, Callable, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, isnan, isnull, when


@dataclass
class QualityCheck:
    """Represents a single data quality check"""
    name: str
    description: str
    check_function: Callable[[DataFrame], Dict[str, Any]]
    severity: str = "ERROR"  # ERROR, WARN, INFO
    enabled: bool = True


@dataclass
class QualityResult:
    """Result of a data quality check"""
    check_name: str
    status: str  # PASS, FAIL, WARN
    message: str
    details: Dict[str, Any]
    severity: str
    execution_time_seconds: float


class BaseQualityCheck(ABC):
    """Base class for quality checks"""
    
    def __init__(self, name: str, description: str, severity: str = "ERROR"):
        self.name = name
        self.description = description
        self.severity = severity
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    @abstractmethod
    def check(self, df: DataFrame) -> Dict[str, Any]:
        """Perform the quality check"""
        pass
    
    def run(self, df: DataFrame) -> QualityResult:
        """Run the quality check and return result"""
        import time
        start_time = time.time()
        
        try:
            result_details = self.check(df)
            status = "PASS" if result_details.get("passed", False) else "FAIL"
            
            # Adjust status based on severity for warnings
            if status == "FAIL" and self.severity == "WARN":
                status = "WARN"
            
            message = result_details.get("message", f"Check {self.name} {'passed' if status == 'PASS' else 'failed'}")
            
        except Exception as e:
            status = "FAIL"
            message = f"Check {self.name} failed with error: {str(e)}"
            result_details = {"error": str(e)}
        
        execution_time = time.time() - start_time
        
        return QualityResult(
            check_name=self.name,
            status=status,
            message=message,
            details=result_details,
            severity=self.severity,
            execution_time_seconds=execution_time
        )


class RequiredColumnsCheck(BaseQualityCheck):
    """Check that required columns exist"""
    
    def __init__(self, required_columns: List[str]):
        super().__init__(
            name="required_columns",
            description=f"Check that required columns exist: {required_columns}"
        )
        self.required_columns = required_columns
    
    def check(self, df: DataFrame) -> Dict[str, Any]:
        missing_columns = set(self.required_columns) - set(df.columns)
        
        return {
            "passed": len(missing_columns) == 0,
            "required_columns": self.required_columns,
            "existing_columns": df.columns,
            "missing_columns": list(missing_columns),
            "message": f"Missing columns: {missing_columns}" if missing_columns else "All required columns present"
        }


class NullValuesCheck(BaseQualityCheck):
    """Check for null values in key columns"""
    
    def __init__(self, key_columns: List[str], allow_nulls: bool = False):
        super().__init__(
            name="null_values",
            description=f"Check for null values in key columns: {key_columns}"
        )
        self.key_columns = key_columns
        self.allow_nulls = allow_nulls
    
    def check(self, df: DataFrame) -> Dict[str, Any]:
        null_counts = {}
        for col_name in self.key_columns:
            if col_name in df.columns:
                null_counts[col_name] = df.filter(col(col_name).isNull()).count()
        
        total_nulls = sum(null_counts.values())
        passed = total_nulls == 0 if not self.allow_nulls else True
        
        return {
            "passed": passed,
            "key_columns": self.key_columns,
            "null_counts": null_counts,
            "total_nulls": total_nulls,
            "message": f"Found {total_nulls} null values" if total_nulls > 0 else "No null values found"
        }


class RowCountCheck(BaseQualityCheck):
    """Check row count is within expected range"""
    
    def __init__(self, min_rows: int = 1, max_rows: Optional[int] = None):
        super().__init__(
            name="row_count",
            description=f"Check row count between {min_rows} and {max_rows or 'unlimited'}"
        )
        self.min_rows = min_rows
        self.max_rows = max_rows
    
    def check(self, df: DataFrame) -> Dict[str, Any]:
        row_count = df.count()
        
        passed = row_count >= self.min_rows
        if self.max_rows is not None:
            passed = passed and row_count <= self.max_rows
        
        return {
            "passed": passed,
            "actual_count": row_count,
            "min_rows": self.min_rows,
            "max_rows": self.max_rows,
            "message": f"Row count {row_count} is {'within' if passed else 'outside'} expected range"
        }


class DuplicatesCheck(BaseQualityCheck):
    """Check for duplicate records based on key columns"""
    
    def __init__(self, key_columns: List[str]):
        super().__init__(
            name="duplicates",
            description=f"Check for duplicate records based on: {key_columns}"
        )
        self.key_columns = key_columns
    
    def check(self, df: DataFrame) -> Dict[str, Any]:
        duplicate_count = df.groupBy(*self.key_columns).count().filter(col("count") > 1).count()
        
        return {
            "passed": duplicate_count == 0,
            "key_columns": self.key_columns,
            "duplicate_count": duplicate_count,
            "message": f"Found {duplicate_count} duplicate records" if duplicate_count > 0 else "No duplicates found"
        }


class DataTypeCheck(BaseQualityCheck):
    """Check data types of columns"""
    
    def __init__(self, expected_types: Dict[str, str]):
        super().__init__(
            name="data_types",
            description=f"Check data types: {expected_types}"
        )
        self.expected_types = expected_types
    
    def check(self, df: DataFrame) -> Dict[str, Any]:
        actual_types = {field.name: str(field.dataType) for field in df.schema.fields}
        type_mismatches = []
        
        for col_name, expected_type in self.expected_types.items():
            if col_name in actual_types:
                if expected_type not in actual_types[col_name]:
                    type_mismatches.append({
                        "column": col_name,
                        "expected": expected_type,
                        "actual": actual_types[col_name]
                    })
        
        return {
            "passed": len(type_mismatches) == 0,
            "expected_types": self.expected_types,
            "actual_types": actual_types,
            "type_mismatches": type_mismatches,
            "message": f"Found {len(type_mismatches)} type mismatches" if type_mismatches else "All types match"
        }


class RangeCheck(BaseQualityCheck):
    """Check values are within expected range"""
    
    def __init__(self, column: str, min_value: Optional[float] = None, max_value: Optional[float] = None):
        super().__init__(
            name=f"range_check_{column}",
            description=f"Check {column} is between {min_value} and {max_value}"
        )
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
    
    def check(self, df: DataFrame) -> Dict[str, Any]:
        if self.column not in df.columns:
            return {
                "passed": False,
                "message": f"Column {self.column} not found"
            }
        
        # Count values outside range
        condition = None
        if self.min_value is not None:
            condition = col(self.column) < self.min_value
        
        if self.max_value is not None:
            max_condition = col(self.column) > self.max_value
            condition = condition | max_condition if condition is not None else max_condition
        
        if condition is not None:
            invalid_count = df.filter(condition).count()
        else:
            invalid_count = 0
        
        return {
            "passed": invalid_count == 0,
            "column": self.column,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "invalid_count": invalid_count,
            "message": f"Found {invalid_count} values outside range" if invalid_count > 0 else "All values within range"
        }


class DataQualityFramework:
    """
    Unified data quality validation framework
    
    Provides a centralized way to define and run data quality checks
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
        self.checks: Dict[str, QualityCheck] = {}
    
    def add_check(self, check: BaseQualityCheck):
        """Add a quality check to the framework"""
        quality_check = QualityCheck(
            name=check.name,
            description=check.description,
            check_function=check.run,
            severity=check.severity
        )
        self.checks[check.name] = quality_check
        self.logger.info(f"Added quality check: {check.name}")
    
    def remove_check(self, check_name: str):
        """Remove a quality check from the framework"""
        if check_name in self.checks:
            del self.checks[check_name]
            self.logger.info(f"Removed quality check: {check_name}")
    
    def run_checks(self, df: DataFrame, check_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run quality checks on a DataFrame
        
        Args:
            df: DataFrame to check
            check_names: Optional list of check names to run. If None, runs all checks.
            
        Returns:
            Dictionary with check results
        """
        checks_to_run = check_names if check_names else list(self.checks.keys())
        
        self.logger.info(f"Running {len(checks_to_run)} quality checks...")
        
        results = {}
        summary = {
            "total_checks": len(checks_to_run),
            "passed": 0,
            "failed": 0,
            "warnings": 0,
            "errors": 0
        }
        
        for check_name in checks_to_run:
            if check_name not in self.checks:
                self.logger.warning(f"Quality check '{check_name}' not found, skipping")
                continue
            
            check = self.checks[check_name]
            if not check.enabled:
                self.logger.info(f"Quality check '{check_name}' is disabled, skipping")
                continue
            
            try:
                result = check.check_function(df)
                results[check_name] = result
                
                # Update summary
                if result.status == "PASS":
                    summary["passed"] += 1
                elif result.status == "WARN":
                    summary["warnings"] += 1
                    summary["failed"] += 1  # Warnings count as failures for overall status
                else:  # FAIL
                    summary["failed"] += 1
                    if result.severity == "ERROR":
                        summary["errors"] += 1
                
                self.logger.info(f"  {check_name}: {result.status}")
                
            except Exception as e:
                self.logger.error(f"Quality check '{check_name}' failed with error: {e}")
                results[check_name] = QualityResult(
                    check_name=check_name,
                    status="FAIL",
                    message=f"Check failed with error: {str(e)}",
                    details={"error": str(e)},
                    severity="ERROR",
                    execution_time_seconds=0
                )
                summary["failed"] += 1
                summary["errors"] += 1
        
        results["summary"] = summary
        
        # Log summary
        self.logger.info(f"Quality check summary: {summary['passed']} passed, {summary['warnings']} warnings, {summary['errors']} errors")
        
        return results
    
    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate a human-readable quality report"""
        if "summary" not in results:
            return "No quality check results available"
        
        summary = results["summary"]
        report_lines = [
            "=" * 60,
            "DATA QUALITY REPORT",
            "=" * 60,
            f"Total Checks: {summary['total_checks']}",
            f"Passed: {summary['passed']}",
            f"Warnings: {summary['warnings']}",
            f"Errors: {summary['errors']}",
            "",
            "DETAILED RESULTS:",
            "-" * 40
        ]
        
        for check_name, result in results.items():
            if check_name == "summary":
                continue
            
            if isinstance(result, QualityResult):
                status_symbol = "✅" if result.status == "PASS" else "⚠️" if result.status == "WARN" else "❌"
                report_lines.extend([
                    f"{status_symbol} {check_name}",
                    f"   Status: {result.status}",
                    f"   Message: {result.message}",
                    f"   Severity: {result.severity}",
                    f"   Execution Time: {result.execution_time_seconds:.2f}s",
                    ""
                ])
        
        return "\n".join(report_lines)
    
    def create_merchants_quality_suite(self) -> 'DataQualityFramework':
        """Create a quality check suite for merchants data"""
        self.add_check(RequiredColumnsCheck(["merchant_id", "merchant_name", "size_category", "status"]))
        self.add_check(NullValuesCheck(["merchant_id", "merchant_name"]))
        self.add_check(RowCountCheck(min_rows=1))
        self.add_check(DuplicatesCheck(["merchant_id"]))
        self.add_check(RangeCheck("mdr_rate", min_value=0.01, max_value=0.10))
        
        return self
    
    def create_transactions_quality_suite(self) -> 'DataQualityFramework':
        """Create a quality check suite for transactions data"""
        self.add_check(RequiredColumnsCheck(["payment_id", "merchant_id", "payment_amount", "payment_status"]))
        self.add_check(NullValuesCheck(["payment_id", "merchant_id"]))
        self.add_check(RowCountCheck(min_rows=1))
        self.add_check(DuplicatesCheck(["payment_id"]))
        self.add_check(RangeCheck("payment_amount", min_value=0.01))
        
        # Check payment status values
        valid_statuses_check = RangeCheck("payment_status")
        valid_statuses_check.name = "valid_payment_status"
        valid_statuses_check.description = "Check payment status values are valid"
        valid_statuses_check.check = lambda df: self._check_valid_statuses(df, "payment_status", ["approved", "declined", "cancelled"])
        self.add_check(valid_statuses_check)
        
        return self
    
    def _check_valid_statuses(self, df: DataFrame, column: str, valid_values: List[str]) -> Dict[str, Any]:
        """Check that column values are in the valid list"""
        if column not in df.columns:
            return {
                "passed": False,
                "message": f"Column {column} not found"
            }
        
        invalid_count = df.filter(~col(column).isin(valid_values)).count()
        
        return {
            "passed": invalid_count == 0,
            "column": column,
            "valid_values": valid_values,
            "invalid_count": invalid_count,
            "message": f"Found {invalid_count} invalid values" if invalid_count > 0 else "All values valid"
        }
