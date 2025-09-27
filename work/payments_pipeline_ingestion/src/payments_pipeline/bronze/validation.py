"""
Data Quality Validation for Bronze Layer

Provides data quality validation functions for ingested data.
"""

import logging
from typing import Dict, List, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, isnan, isnull, when


class DataQualityValidator:
    """
    Data Quality Validator for Bronze Layer
    
    Provides validation functions for data quality checks.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def validate_required_columns(self, df: DataFrame, required_columns: List[str]) -> Dict[str, Any]:
        """
        Validate that required columns exist
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            Validation results dictionary
        """
        missing_columns = []
        existing_columns = df.columns
        
        for col_name in required_columns:
            if col_name not in existing_columns:
                missing_columns.append(col_name)
        
        return {
            "validation_type": "required_columns",
            "required_columns": required_columns,
            "existing_columns": existing_columns,
            "missing_columns": missing_columns,
            "passed": len(missing_columns) == 0
        }
    
    def validate_null_values(self, df: DataFrame, key_columns: List[str]) -> Dict[str, Any]:
        """
        Validate null values in key columns
        
        Args:
            df: DataFrame to validate
            key_columns: List of key column names to check
            
        Returns:
            Validation results dictionary
        """
        null_counts = {}
        
        for col_name in key_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_counts[col_name] = null_count
        
        return {
            "validation_type": "null_values",
            "key_columns": key_columns,
            "null_counts": null_counts,
            "passed": all(count == 0 for count in null_counts.values())
        }
    
    def validate_data_types(self, df: DataFrame, expected_types: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate data types of columns
        
        Args:
            df: DataFrame to validate
            expected_types: Dictionary of column_name -> expected_type
            
        Returns:
            Validation results dictionary
        """
        actual_types = {field.name: str(field.dataType) for field in df.schema.fields}
        type_mismatches = []
        
        for col_name, expected_type in expected_types.items():
            if col_name in actual_types:
                if expected_type not in actual_types[col_name]:
                    type_mismatches.append({
                        "column": col_name,
                        "expected": expected_type,
                        "actual": actual_types[col_name]
                    })
        
        return {
            "validation_type": "data_types",
            "expected_types": expected_types,
            "actual_types": actual_types,
            "type_mismatches": type_mismatches,
            "passed": len(type_mismatches) == 0
        }
    
    def validate_row_count(self, df: DataFrame, min_rows: int = 1, max_rows: int = None) -> Dict[str, Any]:
        """
        Validate row count is within expected range
        
        Args:
            df: DataFrame to validate
            min_rows: Minimum expected rows
            max_rows: Maximum expected rows (optional)
            
        Returns:
            Validation results dictionary
        """
        row_count = df.count()
        
        passed = row_count >= min_rows
        if max_rows is not None:
            passed = passed and row_count <= max_rows
        
        return {
            "validation_type": "row_count",
            "actual_count": row_count,
            "min_rows": min_rows,
            "max_rows": max_rows,
            "passed": passed
        }
    
    def validate_merchants_data(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate merchants data quality
        
        Args:
            df: Merchants DataFrame to validate
            
        Returns:
            Validation results dictionary
        """
        results = {}
        
        # Required columns for merchants
        required_columns = ["merchant_id", "merchant_name", "size_category", "status"]
        results["required_columns"] = self.validate_required_columns(df, required_columns)
        
        # Key columns should not have nulls
        key_columns = ["merchant_id", "merchant_name"]
        results["null_validation"] = self.validate_null_values(df, key_columns)
        
        # Row count validation
        results["row_count"] = self.validate_row_count(df, min_rows=1)
        
        # Check for duplicate merchant IDs
        duplicate_count = df.groupBy("merchant_id").count().filter(col("count") > 1).count()
        results["duplicates"] = {
            "validation_type": "duplicate_merchant_ids",
            "duplicate_count": duplicate_count,
            "passed": duplicate_count == 0
        }
        
        # Overall validation result
        all_passed = all(
            result.get("passed", False) 
            for result in results.values() 
            if isinstance(result, dict) and "passed" in result
        )
        results["overall_passed"] = all_passed
        
        return results
    
    def validate_transactions_data(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate transactions data quality
        
        Args:
            df: Transactions DataFrame to validate
            
        Returns:
            Validation results dictionary
        """
        results = {}
        
        # Required columns for transactions
        required_columns = ["payment_id", "merchant_id", "payment_amount", "payment_status"]
        results["required_columns"] = self.validate_required_columns(df, required_columns)
        
        # Key columns should not have nulls
        key_columns = ["payment_id", "merchant_id"]
        results["null_validation"] = self.validate_null_values(df, key_columns)
        
        # Row count validation
        results["row_count"] = self.validate_row_count(df, min_rows=1)
        
        # Check for duplicate payment IDs
        duplicate_count = df.groupBy("payment_id").count().filter(col("count") > 1).count()
        results["duplicates"] = {
            "validation_type": "duplicate_payment_ids",
            "duplicate_count": duplicate_count,
            "passed": duplicate_count == 0
        }
        
        # Validate payment amounts are positive
        negative_amounts = df.filter(col("payment_amount") <= 0).count()
        results["amount_validation"] = {
            "validation_type": "positive_amounts",
            "negative_count": negative_amounts,
            "passed": negative_amounts == 0
        }
        
        # Validate payment status values
        valid_statuses = ["approved", "declined", "cancelled"]
        invalid_statuses = df.filter(~col("payment_status").isin(valid_statuses)).count()
        results["status_validation"] = {
            "validation_type": "valid_statuses",
            "invalid_count": invalid_statuses,
            "valid_statuses": valid_statuses,
            "passed": invalid_statuses == 0
        }
        
        # Overall validation result
        all_passed = all(
            result.get("passed", False) 
            for result in results.values() 
            if isinstance(result, dict) and "passed" in result
        )
        results["overall_passed"] = all_passed
        
        return results
    
    def run_comprehensive_validation(self, table_name: str) -> Dict[str, Any]:
        """
        Run comprehensive validation on a table
        
        Args:
            table_name: Name of the table to validate
            
        Returns:
            Comprehensive validation results
        """
        self.logger.info(f"🔍 Running comprehensive validation on {table_name}")
        
        try:
            df = self.spark.table(table_name)
            
            validation_results = {
                "table_name": table_name,
                "timestamp": self.spark.sql("SELECT current_timestamp()").collect()[0][0],
                "basic_stats": {
                    "row_count": df.count(),
                    "column_count": len(df.columns),
                    "columns": df.columns
                }
            }
            
            # Run specific validations based on table type
            if "merchants" in table_name.lower():
                validation_results["data_quality"] = self.validate_merchants_data(df)
            elif "transactions" in table_name.lower():
                validation_results["data_quality"] = self.validate_transactions_data(df)
            else:
                # Generic validation
                validation_results["data_quality"] = {
                    "required_columns": self.validate_required_columns(df, ["id"]),
                    "row_count": self.validate_row_count(df),
                    "overall_passed": True
                }
            
            self.logger.info(f"✅ Validation completed for {table_name}")
            return validation_results
            
        except Exception as e:
            self.logger.error(f"❌ Validation failed for {table_name}: {e}")
            return {
                "table_name": table_name,
                "error": str(e),
                "overall_passed": False
            }
