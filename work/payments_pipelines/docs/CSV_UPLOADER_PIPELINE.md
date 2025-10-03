# CSV Uploader Pipeline

This document describes the `CSVUploaderPipeline`, a focused pipeline component that handles CSV file upload and state management as part of the modular architecture.

## 🎯 Purpose

The `CSVUploaderPipeline` addresses the **separation of concerns** principle by extracting file upload and state management from the monolithic bronze ingestion process. This pipeline:

- **Handles file discovery** based on configurable patterns
- **Manages upload state** to track processed files
- **Performs incremental uploads** (only new/changed files)
- **Coordinates S3/MinIO uploads** using existing utilities
- **Provides state persistence** for downstream pipelines

## 🏗️ Architecture Benefits

### **Before (Monolithic)**
```
BronzeIngestionJob
├── File discovery
├── State management
├── S3 upload
├── Data ingestion
├── Quality checks
└── Table creation
```

### **After (Modular)**
```
CSVUploaderPipeline          MerchantsBronzePipeline
├── File discovery           ├── Data ingestion
├── State management         ├── Quality checks
└── S3 upload               └── Table creation
         ↓
    (dependency)
```

## 📁 Implementation

### **Core Class: `CSVUploaderPipeline`**

**Location:** `src/payments_pipeline/bronze/csv_uploader_pipeline.py`

**Key Methods:**
- `execute()` - Main pipeline execution
- `_discover_files()` - Find files matching patterns
- `_upload_files()` - Upload files to S3/MinIO
- `_load_state()` / `_save_state()` - State management
- `_get_file_hash()` - Change detection
- `get_uploaded_files_info()` - State introspection

### **Configuration**

**File:** `pipeline_configs/development.yml`

```yaml
csv_upload:
  description: "CSV file upload and state management"
  enabled: true
  pipelines:
    csv_uploader:
      class_name: "CSVUploaderPipeline"
      config:
        source_config:
          base_path: "/usr/local/spark_dev/work/payments_data_source/raw_data"
          file_patterns: ["merchants_*.csv", "transactions_*.csv"]
        state_file_name: "upload_state.json"
        cleanup_after_upload: true
        keep_days: 0  # Remove files after upload
        enable_debug_logging: true
```

### **Dependency Management**

The `MerchantsBronzePipeline` now depends on `CSVUploaderPipeline`:

```yaml
modular_merchants_bronze:
  class_name: "MerchantsBronzePipeline"
  dependencies: ["csv_uploader"]  # Depends on CSV upload pipeline
  config:
    # ... pipeline configuration
```

## 🚀 Usage

### **1. Individual Pipeline Execution**

```bash
# Run only CSV uploader
docker-compose exec workspace python3 /usr/local/spark_dev/work/payments_pipelines/scripts/run_csv_uploader.py
```

### **2. Full Pipeline Chain**

```bash
# Run CSV upload → Merchants bronze (with dependency resolution)
docker-compose exec workspace python3 /usr/local/spark_dev/work/payments_pipelines/scripts/run_full_pipeline_chain.py --mode full
```

### **3. Selective Execution**

```bash
# Run only CSV upload
python3 run_full_pipeline_chain.py --mode csv-only

# Run only merchants bronze (assumes CSV upload completed)
python3 run_full_pipeline_chain.py --mode merchants-only
```

## 🔄 State Management

### **State File Structure**

The pipeline maintains state in `upload_state.json`:

```json
{
  "processed_files": {
    "merchants_2024-01-01_2024-03-31.csv": {
      "hash": "12345_1640995200.0",
      "uploaded_at": "2024-01-01T10:00:00",
      "file_size": 1024
    }
  },
  "last_updated": "2024-01-01T10:00:00"
}
```

### **Change Detection**

Files are considered "new" if:
- File doesn't exist in state
- File hash (size + modification time) has changed

### **Incremental Processing**

Only new/changed files are uploaded, enabling:
- **Efficient processing** of large datasets
- **Resumable operations** after failures
- **State consistency** across pipeline runs

## 🔧 Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `source_config.base_path` | Directory containing CSV files | Required |
| `source_config.file_patterns` | File patterns to match | `["*.csv"]` |
| `state_file_name` | State file name | `"upload_state.json"` |
| `cleanup_after_upload` | Remove S3 files after upload | `true` |
| `keep_days` | Days to keep S3 files (0 = remove all) | `0` |

## 🧪 Testing

The pipeline can be tested independently:

```python
from payments_pipeline.bronze.csv_uploader_pipeline import CSVUploaderPipeline

config = {
    "source_config": {
        "base_path": "/path/to/data",
        "file_patterns": ["*.csv"]
    }
}

pipeline = CSVUploaderPipeline(config, spark_session)
result = pipeline.execute()
```

## 🔗 Integration with Orchestrator

The `PipelineOrchestrator` automatically:

1. **Resolves dependencies** - CSV uploader runs before merchants bronze
2. **Manages execution order** - Based on dependency graph
3. **Handles failures** - Stops chain if dependency fails
4. **Provides monitoring** - Execution summary and metrics

## 📊 Monitoring

### **Pipeline Metrics**

```python
# Get upload statistics
upload_info = pipeline.get_uploaded_files_info()
print(f"Total processed: {upload_info['total_processed']}")
print(f"Last updated: {upload_info['last_updated']}")
```

### **Execution Summary**

```
CSV Uploader Pipeline Execution Summary:
============================================================
Pipeline: csv_uploader
Status: SUCCESS
Duration: 2.5 seconds
Files processed: 4
Successful uploads: 4
Failed uploads: 0
```

## 🎉 Benefits Achieved

1. **✅ Separation of Concerns** - Upload logic separated from ingestion
2. **✅ Reusability** - CSV uploader can serve multiple downstream pipelines
3. **✅ Testability** - Each component can be tested independently
4. **✅ State Management** - Proper tracking of processed files
5. **✅ Dependency Management** - Clean orchestration of pipeline chains
6. **✅ Incremental Processing** - Only process new/changed files
7. **✅ Error Isolation** - Upload failures don't affect ingestion logic

This modular design demonstrates how the refactored architecture enables **composition over monolithic design**, making each pipeline a **focused, reusable component** that can be orchestrated through clear dependencies.
