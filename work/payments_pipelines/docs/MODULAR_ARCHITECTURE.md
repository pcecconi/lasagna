# Modular Pipeline Architecture

This document describes the new modular architecture for the payments pipeline system, designed to be more maintainable, reusable, and production-ready.

## Overview

The new architecture addresses the limitations of the monolithic pipeline approach by:

1. **Extracting Common Utilities**: Reusable components for data quality, schema management, and table operations
2. **Base Classes and Mixins**: Common functionality inherited by all pipelines
3. **YAML Configuration**: Declarative pipeline definitions with dependencies
4. **Simple Orchestration**: Dependency management and execution coordination
5. **Separation of Concerns**: Distinct pipeline classes for different data types

## Architecture Components

### 1. Base Classes and Mixins

#### `BasePipeline`
- Abstract base class for all pipeline implementations
- Provides common functionality: configuration, logging, error handling, metrics
- Defines the standard `execute()` method interface

#### `DataIngestionMixin`
- Common data ingestion patterns
- Metadata addition (ingestion timestamp, source file, etc.)
- Schema validation
- Error handling for data issues

#### `TableManagementMixin`
- Iceberg table creation and management
- Namespace management
- Table verification and cleanup

#### `DataQualityMixin`
- Common data quality validation patterns
- Null value checking
- Duplicate detection
- Row count validation

### 2. Data Quality Framework

#### `DataQualityFramework`
- Unified framework for data quality validation
- Pluggable quality checks
- Configurable severity levels (ERROR, WARN, INFO)
- Comprehensive reporting

#### Built-in Quality Checks
- `RequiredColumnsCheck`: Validate required columns exist
- `NullValuesCheck`: Check for null values in key columns
- `RowCountCheck`: Validate row count within expected range
- `DuplicatesCheck`: Detect duplicate records
- `DataTypeCheck`: Validate data types
- `RangeCheck`: Check values within expected ranges

### 3. Schema Management

#### `SchemaManager`
- Schema validation and evolution
- YAML-based schema storage
- Type compatibility checking
- Automatic schema migration

### 4. Configuration System

#### `PipelineConfigManager`
- YAML-based pipeline definitions
- Dependency resolution
- Configuration inheritance
- Validation and error checking

**Simplified Design**: The configuration focuses only on what's actually needed - pipeline dependencies and configuration parameters. Input/output declarations were removed as they added complexity without providing real value for our use case.

#### Configuration Structure
```yaml
version: "1.0"
description: "Pipeline Configuration"

pipeline_groups:
  bronze_layer:
    description: "Bronze layer pipelines"
    enabled: true
    pipelines:
      bronze_merchants:
        class_name: "BronzeMerchantsPipeline"
        dependencies: []
        config:
          table_name: "merchants_raw"
          source_config:
            base_path: "/path/to/data"
            file_pattern: "merchants_*.csv"
          quality_checks: ["required_columns", "null_values"]
          batch_size: 10000
          enable_debug_logging: true
        enabled: true
```

#### Environment-Specific Configurations
```yaml
# development.yml
bronze_merchants:
  config:
    source_config:
      base_path: "/local/dev/data"
    batch_size: 1000
    enable_debug_logging: true

# production.yml  
bronze_merchants:
  config:
    source_config:
      base_path: "s3://prod-bucket/data"
    batch_size: 100000
    enable_debug_logging: false
    compression: "snappy"
    max_retries: 3
```

### 5. Pipeline Orchestrator

#### `PipelineOrchestrator`
- Dependency resolution and execution ordering
- Sequential pipeline execution
- Error handling and rollback
- Execution monitoring and reporting

## Usage Examples

### Creating a New Pipeline

```python
from payments_pipeline.common.base_pipeline import BasePipeline, DataIngestionMixin, TableManagementMixin, DataQualityMixin

class MyCustomPipeline(BasePipeline, DataIngestionMixin, TableManagementMixin, DataQualityMixin):
    def __init__(self, config=None, spark_session=None, pipeline_name="MyCustomPipeline"):
        super().__init__(config, spark_session, pipeline_name)
        
        # Initialize components
        self.schema_manager = SchemaManager(self.spark)
        self.quality_framework = DataQualityFramework(self.spark)
        
        # Set up quality checks
        self._setup_quality_checks()
    
    def _setup_quality_checks(self):
        self.quality_framework.add_check(RequiredColumnsCheck(["id", "name"]))
        self.quality_framework.add_check(NullValuesCheck(["id"]))
    
    def execute(self):
        # Implementation here
        pass
```

### Using the Orchestrator

```python
# Initialize orchestrator
orchestrator = PipelineOrchestrator(spark)

# Register pipeline classes
orchestrator.register_pipeline_class("BronzeMerchantsPipeline", BronzeMerchantsPipeline)
orchestrator.register_pipeline_class("BronzeTransactionsPipeline", BronzeTransactionsPipeline)

# Load configuration - multiple options
orchestrator.load_configuration(config_file="pipeline_configs/payments_pipelines.yml")  # Specific file
orchestrator.load_configuration(environment="development")  # Environment-specific
orchestrator.load_configuration(environment="production")   # Production config

# Execute pipeline group
results = orchestrator.execute_pipeline_group("bronze_layer")

# Get execution summary
summary = orchestrator.get_execution_summary()
```

### Configuration-Driven Pipelines

```python
class BronzeMerchantsPipeline(BasePipeline, ...):
    def validate_inputs(self) -> bool:
        # Get configuration values
        source_config = self.config.get("source_config", {})
        base_path = source_config.get("base_path", "/default/path")
        file_pattern = source_config.get("file_pattern", "*.csv")
        
        # Use configuration for validation
        source_files = list(Path(base_path).glob(file_pattern))
        return len(source_files) > 0
    
    def execute(self):
        # Get any custom configuration
        batch_size = self.config.get("batch_size", 10000)
        enable_debug = self.config.get("enable_debug_logging", False)
        
        # Pipeline logic uses these values
        pass
```

### Data Quality Validation

```python
# Set up quality framework
quality_framework = DataQualityFramework(spark)
quality_framework.add_check(RequiredColumnsCheck(["merchant_id", "merchant_name"]))
quality_framework.add_check(NullValuesCheck(["merchant_id"]))
quality_framework.add_check(RangeCheck("mdr_rate", min_value=0.01, max_value=0.10))

# Run checks
results = quality_framework.run_checks(df)

# Generate report
report = quality_framework.generate_report(results)
print(report)
```

## Benefits

### 1. **Reusability**
- Common patterns extracted into reusable components
- Mixins provide specific functionality without inheritance complexity
- Quality checks can be reused across different pipelines

### 2. **Maintainability**
- Clear separation of concerns
- Consistent error handling and logging
- Standardized interfaces

### 3. **Testability**
- Each component can be tested independently
- Mock-friendly interfaces
- Clear dependencies

### 4. **Configuration-Driven**
- Pipelines defined declaratively in YAML
- Easy to modify without code changes
- Dependency management handled automatically

### 5. **Production-Ready**
- Comprehensive error handling and rollback
- Quality validation framework
- Execution monitoring and reporting
- Schema evolution support

## Migration Path

The new architecture is designed to coexist with the existing system:

1. **Phase 1**: Extract common utilities (✅ Completed)
2. **Phase 2**: Create base classes and mixins (✅ Completed)
3. **Phase 3**: Implement YAML configuration system (✅ Completed)
4. **Phase 4**: Separate merchants and transactions pipelines (🔄 In Progress)
5. **Phase 5**: Update tests for new architecture (🔄 Pending)

## Future Enhancements

1. **Advanced Orchestration**: Add parallel execution, retry logic, and monitoring
2. **Pipeline Templates**: Pre-built templates for common patterns
3. **Dynamic Configuration**: Runtime configuration updates
4. **Metrics and Monitoring**: Integration with monitoring systems
5. **Pipeline Versioning**: Version management for pipeline definitions

## Files Structure

```
src/payments_pipeline/
├── common/
│   ├── __init__.py
│   ├── base_pipeline.py          # Base classes and mixins
│   ├── data_quality.py          # Data quality framework
│   ├── schema_manager.py        # Schema management
│   ├── pipeline_config.py       # Configuration management
│   └── pipeline_orchestrator.py # Orchestration
├── bronze/                      # Existing bronze layer
├── silver/                      # Existing silver layer
└── utils/                       # Existing utilities

pipeline_configs/
└── payments_pipelines.yml       # Pipeline configuration

examples/
└── modular_pipeline_example.py  # Usage examples
```

This modular architecture provides a solid foundation for building maintainable, scalable data pipelines while maintaining backward compatibility with the existing system.
