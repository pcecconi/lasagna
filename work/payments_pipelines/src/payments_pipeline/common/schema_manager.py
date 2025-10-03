#!/usr/bin/env python3
"""
Schema Management

Provides schema validation, evolution, and management functionality.
"""

import logging
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, DataType
import yaml


class SchemaManager:
    """
    Manages data schemas for pipeline tables
    
    Provides functionality for:
    - Schema validation
    - Schema evolution
    - Schema storage and retrieval
    """
    
    def __init__(self, spark_session: SparkSession, schema_dir: Optional[str] = None):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
        self.schema_dir = Path(schema_dir) if schema_dir else Path("schemas")
        self.schema_dir.mkdir(exist_ok=True)
        
        # Cache for loaded schemas
        self._schema_cache: Dict[str, StructType] = {}
    
    def get_schema(self, table_name: str) -> Optional[StructType]:
        """
        Get schema for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            StructType if schema exists, None otherwise
        """
        # Check cache first
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]
        
        # Try to get from existing table
        try:
            df = self.spark.table(table_name)
            schema = df.schema
            self._schema_cache[table_name] = schema
            return schema
        except Exception as e:
            self.logger.warning(f"Could not get schema from table {table_name}: {e}")
        
        # Try to load from schema file
        schema_file = self.schema_dir / f"{table_name}.yaml"
        if schema_file.exists():
            schema = self._load_schema_from_file(schema_file)
            if schema:
                self._schema_cache[table_name] = schema
                return schema
        
        return None
    
    def validate_schema(self, df: DataFrame, expected_schema: Union[str, StructType], 
                       strict: bool = False) -> Dict[str, Any]:
        """
        Validate DataFrame schema against expected schema
        
        Args:
            df: DataFrame to validate
            expected_schema: Expected schema (table name or StructType)
            strict: If True, requires exact match. If False, allows extra columns.
            
        Returns:
            Validation results dictionary
        """
        # Get expected schema
        if isinstance(expected_schema, str):
            expected = self.get_schema(expected_schema)
            if expected is None:
                return {
                    "passed": False,
                    "message": f"Could not find schema for {expected_schema}",
                    "errors": [f"Schema not found: {expected_schema}"]
                }
        else:
            expected = expected_schema
        
        actual_schema = df.schema
        errors = []
        warnings = []
        
        # Check field names and types
        expected_fields = {field.name: field for field in expected.fields}
        actual_fields = {field.name: field for field in actual_schema.fields}
        
        # Check for missing required fields
        for field_name, expected_field in expected_fields.items():
            if field_name not in actual_fields:
                errors.append(f"Missing required field: {field_name}")
            else:
                actual_field = actual_fields[field_name]
                # Check data type compatibility
                if not self._is_compatible_type(actual_field.dataType, expected_field.dataType):
                    errors.append(f"Incompatible type for {field_name}: expected {expected_field.dataType}, got {actual_field.dataType}")
        
        # Check for extra fields (if strict mode)
        if strict:
            for field_name in actual_fields:
                if field_name not in expected_fields:
                    errors.append(f"Unexpected field: {field_name}")
        
        # Warnings for extra fields (if not strict)
        if not strict:
            for field_name in actual_fields:
                if field_name not in expected_fields:
                    warnings.append(f"Extra field: {field_name}")
        
        passed = len(errors) == 0
        message = "Schema validation passed" if passed else f"Schema validation failed with {len(errors)} errors"
        
        return {
            "passed": passed,
            "message": message,
            "errors": errors,
            "warnings": warnings,
            "expected_fields": len(expected_fields),
            "actual_fields": len(actual_fields),
            "missing_fields": len(errors) if errors else 0,
            "extra_fields": len(warnings) if warnings else 0
        }
    
    def evolve_schema(self, df: DataFrame, target_schema: Union[str, StructType]) -> DataFrame:
        """
        Evolve DataFrame schema to match target schema
        
        Args:
            df: Source DataFrame
            target_schema: Target schema (table name or StructType)
            
        Returns:
            DataFrame with evolved schema
        """
        # Get target schema
        if isinstance(target_schema, str):
            target = self.get_schema(target_schema)
            if target is None:
                raise ValueError(f"Could not find schema for {target_schema}")
        else:
            target = target_schema
        
        current_schema = df.schema
        evolved_df = df
        
        # Add missing columns with default values
        current_field_names = {field.name for field in current_schema.fields}
        target_field_names = {field.name for field in target.fields}
        
        missing_fields = target_field_names - current_field_names
        
        if missing_fields:
            self.logger.info(f"Adding missing fields: {missing_fields}")
            for field in target.fields:
                if field.name in missing_fields:
                    default_value = self._get_default_value(field.dataType)
                    evolved_df = evolved_df.withColumn(field.name, default_value)
        
        # Cast columns to correct types
        for target_field in target.fields:
            if target_field.name in evolved_df.columns:
                current_field = next((f for f in current_schema.fields if f.name == target_field.name), None)
                if current_field and current_field.dataType != target_field.dataType:
                    if self._is_compatible_type(current_field.dataType, target_field.dataType):
                        self.logger.info(f"Casting {target_field.name} from {current_field.dataType} to {target_field.dataType}")
                        from pyspark.sql.functions import col
                        evolved_df = evolved_df.withColumn(target_field.name, col(target_field.name).cast(target_field.dataType))
        
        # Select columns in target order
        target_columns = [field.name for field in target.fields]
        evolved_df = evolved_df.select(*target_columns)
        
        return evolved_df
    
    def save_schema(self, table_name: str, schema: StructType):
        """Save schema to YAML file"""
        schema_file = self.schema_dir / f"{table_name}.yaml"
        
        schema_dict = {
            "table_name": table_name,
            "fields": [
                {
                    "name": field.name,
                    "type": str(field.dataType),
                    "nullable": field.nullable,
                    "metadata": field.metadata
                }
                for field in schema.fields
            ]
        }
        
        with open(schema_file, 'w') as f:
            yaml.dump(schema_dict, f, default_flow_style=False)
        
        self.logger.info(f"Saved schema for {table_name} to {schema_file}")
    
    def load_schema(self, table_name: str) -> Optional[StructType]:
        """Load schema from YAML file"""
        schema_file = self.schema_dir / f"{table_name}.yaml"
        
        if not schema_file.exists():
            return None
        
        return self._load_schema_from_file(schema_file)
    
    def _load_schema_from_file(self, schema_file: Path) -> Optional[StructType]:
        """Load schema from YAML file"""
        try:
            with open(schema_file, 'r') as f:
                schema_dict = yaml.safe_load(f)
            
            fields = []
            for field_dict in schema_dict.get("fields", []):
                # Convert string type to DataType
                field_type = self._string_to_datatype(field_dict["type"])
                field = StructField(
                    name=field_dict["name"],
                    dataType=field_type,
                    nullable=field_dict.get("nullable", True),
                    metadata=field_dict.get("metadata", {})
                )
                fields.append(field)
            
            return StructType(fields)
            
        except Exception as e:
            self.logger.error(f"Error loading schema from {schema_file}: {e}")
            return None
    
    def _is_compatible_type(self, actual_type: DataType, expected_type: DataType) -> bool:
        """Check if actual type is compatible with expected type"""
        # Simple compatibility check - can be extended
        type_hierarchy = {
            "StringType": ["StringType"],
            "IntegerType": ["IntegerType", "LongType", "DoubleType", "StringType"],
            "LongType": ["LongType", "DoubleType", "StringType"],
            "DoubleType": ["DoubleType", "StringType"],
            "BooleanType": ["BooleanType", "StringType"],
            "DateType": ["DateType", "StringType", "TimestampType"],
            "TimestampType": ["TimestampType", "StringType", "DateType"]
        }
        
        actual_str = str(actual_type)
        expected_str = str(expected_type)
        
        # Extract base type name (remove nullable info)
        actual_base = actual_str.split('(')[0].replace('Type', 'Type')
        expected_base = expected_str.split('(')[0].replace('Type', 'Type')
        
        if actual_base == expected_base:
            return True
        
        # Check compatibility hierarchy
        if expected_base in type_hierarchy:
            return actual_base in type_hierarchy[expected_base]
        
        return False
    
    def _get_default_value(self, data_type: DataType) -> Any:
        """Get default value for a data type"""
        from pyspark.sql.functions import lit
        
        type_str = str(data_type)
        
        if "StringType" in type_str:
            return lit("")
        elif "IntegerType" in type_str or "LongType" in type_str:
            return lit(0)
        elif "DoubleType" in type_str or "FloatType" in type_str:
            return lit(0.0)
        elif "BooleanType" in type_str:
            return lit(False)
        elif "DateType" in type_str:
            return lit(None)  # Will be null
        elif "TimestampType" in type_str:
            return lit(None)  # Will be null
        else:
            return lit(None)  # Default to null
    
    def _string_to_datatype(self, type_str: str) -> DataType:
        """Convert string representation to DataType"""
        from pyspark.sql.types import (
            StringType, IntegerType, LongType, DoubleType, FloatType,
            BooleanType, DateType, TimestampType, DecimalType, ArrayType, MapType
        )
        
        type_mapping = {
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "LongType": LongType(),
            "DoubleType": DoubleType(),
            "FloatType": FloatType(),
            "BooleanType": BooleanType(),
            "DateType": DateType(),
            "TimestampType": TimestampType(),
        }
        
        if type_str in type_mapping:
            return type_mapping[type_str]
        
        # Handle nullable types
        if type_str.startswith("StringType(") and type_str.endswith(")"):
            return StringType()
        
        # Default to StringType for unknown types
        self.logger.warning(f"Unknown type {type_str}, defaulting to StringType")
        return StringType()
    
    def create_merchants_schema(self) -> StructType:
        """Create merchants table schema"""
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, DoubleType, DateType
        )
        
        return StructType([
            StructField("merchant_id", StringType(), False),
            StructField("merchant_name", StringType(), False),
            StructField("industry", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", IntegerType(), True),
            StructField("phone", StringType(), True),
            StructField("email", StringType(), True),
            StructField("mdr_rate", DoubleType(), True),
            StructField("size_category", StringType(), True),
            StructField("creation_date", DateType(), True),
            StructField("effective_date", DateType(), True),
            StructField("status", StringType(), True),
            StructField("last_transaction_date", StringType(), True),
            StructField("version", IntegerType(), True),
            StructField("change_type", StringType(), True),
            StructField("churn_date", DateType(), True),
        ])
    
    def create_transactions_schema(self) -> StructType:
        """Create transactions table schema"""
        from pyspark.sql.types import (
            StructType, StructField, StringType, TimestampType, DoubleType
        )
        
        return StructType([
            StructField("payment_id", StringType(), False),
            StructField("payment_timestamp", TimestampType(), False),
            StructField("payment_lat", DoubleType(), True),
            StructField("payment_lng", DoubleType(), True),
            StructField("payment_amount", DoubleType(), False),
            StructField("payment_type", StringType(), True),
            StructField("terminal_id", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("card_issuer", StringType(), True),
            StructField("card_brand", StringType(), True),
            StructField("card_profile_id", StringType(), True),
            StructField("card_bin", StringType(), True),
            StructField("payment_status", StringType(), False),
            StructField("merchant_id", StringType(), False),
            StructField("transactional_cost_rate", DoubleType(), True),
            StructField("transactional_cost_amount", DoubleType(), True),
            StructField("mdr_amount", DoubleType(), True),
            StructField("net_profit", DoubleType(), True),
        ])
