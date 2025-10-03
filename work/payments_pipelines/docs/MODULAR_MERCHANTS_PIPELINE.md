# Modular Merchants Bronze Pipeline

This document describes the new modular merchants bronze ingestion pipeline that demonstrates the modular architecture while coexisting with the legacy pipeline.

## 🎯 **Overview**

The modular merchants pipeline (`MerchantsBronzePipeline`) is an independent implementation that:

- **Creates a separate table**: `iceberg.payments_bronze.merchants` (vs legacy `merchants_raw`)
- **Uses modular architecture**: Leverages base classes, mixins, and common components
- **Coexists with legacy**: Both pipelines can run independently without conflicts
- **Provides comprehensive testing**: Allows comparison between old and new approaches

## 🏗️ **Architecture**

### **Pipeline Class**
```python
class MerchantsBronzePipeline(BasePipeline, DataIngestionMixin, TableManagementMixin, DataQualityMixin)
```

### **Key Components**
- **BasePipeline**: Core pipeline functionality and lifecycle management
- **DataIngestionMixin**: Data loading, metadata addition, error handling
- **TableManagementMixin**: Iceberg table creation and management
- **DataQualityMixin**: Comprehensive data quality validation
- **SchemaManager**: Schema creation and validation
- **DataQualityFramework**: Reusable quality check components

### **Table Structure**
```sql
CREATE TABLE iceberg.payments_bronze.merchants (
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,
    merchant_address STRING,
    merchant_city STRING,
    merchant_state STRING,
    merchant_country STRING,
    merchant_phone STRING,
    merchant_email STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    -- Metadata columns added by pipeline
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    bronze_layer_version STRING,
    data_source STRING
) USING ICEBERG
PARTITIONED BY (merchant_country, merchant_state)
```

## 🚀 **Usage**

### **1. Direct Pipeline Execution**
```python
from payments_pipeline.bronze.merchants_pipeline import MerchantsBronzePipeline

# Configuration
config = {
    "catalog": "iceberg",
    "database": "payments_bronze",
    "table_name": "merchants",
    "source_config": {
        "base_path": "/path/to/merchant/data",
        "file_pattern": "merchants_*.csv"
    }
}

# Create and run pipeline
pipeline = MerchantsBronzePipeline(config, spark_session)
result = pipeline.execute()
```

### **2. Using Pipeline Orchestrator**
```python
from payments_pipeline.common.pipeline_orchestrator import PipelineOrchestrator

# Register pipeline class
orchestrator = PipelineOrchestrator(spark_session)
orchestrator.register_pipeline_class(MerchantsBronzePipeline)

# Load configuration and run
orchestrator.load_configuration("pipeline_configs/development.yml")
result = orchestrator.execute_single_pipeline("modular_merchants_bronze")
```

### **3. Command Line Execution**
```bash
# Run the modular pipeline
python scripts/run_modular_merchants.py

# Compare with legacy pipeline
python scripts/compare_pipelines.py

# Run comprehensive example
python examples/modular_merchants_example.py
```

## 📋 **Configuration**

### **YAML Configuration**
```yaml
modular_merchants_bronze:
  class_name: "MerchantsBronzePipeline"
  config:
    table_name: "merchants"  # Independent table name
    database: "payments_bronze"
    catalog: "iceberg"
    source_config:
      base_path: "/usr/local/spark_dev/work/payments_data_source/raw_data"
      file_pattern: "merchants_*.csv"
    quality_checks: ["required_columns", "null_values", "duplicates"]
    batch_size: 1000
    enable_debug_logging: true
  enabled: true
```

### **Configuration Parameters**
- **table_name**: Target table name (`merchants` for modular, `merchants_raw` for legacy)
- **database**: Target database (`payments_bronze`)
- **catalog**: Iceberg catalog name (`iceberg`)
- **source_config**: Source data configuration
  - **base_path**: Path to merchant CSV files
  - **file_pattern**: File pattern to match (`merchants_*.csv`)
- **quality_checks**: List of quality checks to run
- **batch_size**: Processing batch size
- **enable_debug_logging**: Enable detailed logging

## 🔍 **Data Quality Checks**

The modular pipeline includes comprehensive data quality validation:

### **Required Columns Check**
- Validates all required merchant fields are present
- Checks for missing critical columns

### **Null Values Check**
- Identifies null values in key columns
- Configurable null tolerance per column

### **Duplicates Check**
- Detects duplicate merchant records
- Uses merchant_id as the primary key

### **Custom Quality Framework**
```python
# Create merchants-specific quality suite
quality_framework = DataQualityFramework(spark)
quality_framework.create_merchants_quality_suite()

# Run comprehensive checks
results = quality_framework.run_checks(merchants_df)
```

## 📊 **Pipeline Execution Flow**

1. **Input Validation**: Verify configuration and source data
2. **Namespace Creation**: Create `payments_bronze` namespace if needed
3. **Data Loading**: Load merchant data from CSV files
4. **Metadata Addition**: Add ingestion metadata columns
5. **Quality Checks**: Run comprehensive data quality validation
6. **Table Creation**: Create Iceberg table with proper partitioning
7. **Data Writing**: Write processed data to the table
8. **Verification**: Verify ingestion results and data integrity

## 🔄 **Coexistence with Legacy Pipeline**

### **Independent Tables**
- **Legacy**: `iceberg.payments_bronze.merchants_raw`
- **Modular**: `iceberg.payments_bronze.merchants`

### **Comparison Tools**
```bash
# Compare both pipelines
python scripts/compare_pipelines.py

# Expected output:
# ✅ Legacy table 'merchants_raw': 1000 records
# ✅ Modular table 'merchants': 1000 records
# ✅ Record counts match!
```

### **Validation Steps**
1. Run legacy pipeline → creates `merchants_raw` table
2. Run modular pipeline → creates `merchants` table
3. Compare results → verify equivalent data
4. Both tables coexist independently

## 🧪 **Testing and Validation**

### **Unit Tests**
```bash
# Run modular pipeline tests
python -m pytest tests/unit/test_merchants_pipeline.py -v
```

### **Integration Tests**
```bash
# Run full pipeline comparison
python scripts/compare_pipelines.py
```

### **Example Execution**
```bash
# Run comprehensive example
python examples/modular_merchants_example.py
```

## 📈 **Benefits of Modular Architecture**

### **Reusability**
- Common components can be reused across different pipelines
- Mixins provide shared functionality
- Configuration-driven behavior

### **Maintainability**
- Clear separation of concerns
- Modular components are easier to test
- Consistent patterns across pipelines

### **Extensibility**
- Easy to add new quality checks
- Simple to extend with new mixins
- Configuration-driven customization

### **Testability**
- Each component can be tested independently
- Comprehensive test coverage
- Easy to mock dependencies

## 🔧 **Troubleshooting**

### **Common Issues**

1. **Table Already Exists**
   ```python
   # The pipeline uses CREATE TABLE IF NOT EXISTS
   # No conflicts with existing tables
   ```

2. **Source Data Not Found**
   ```python
   # Check source path configuration
   config["source_config"]["base_path"] = "/correct/path"
   ```

3. **Quality Checks Failing**
   ```python
   # Review data quality results
   results = quality_framework.generate_report()
   print(results)
   ```

### **Debug Mode**
```python
config["enable_debug_logging"] = True
```

## 🚀 **Next Steps**

1. **Run the modular pipeline** to create the `merchants` table
2. **Compare with legacy results** using the comparison script
3. **Extend the architecture** to other data sources
4. **Add more quality checks** as needed
5. **Migrate other pipelines** to the modular architecture

The modular merchants pipeline demonstrates how the new architecture provides better maintainability, testability, and reusability while coexisting peacefully with existing legacy systems.
