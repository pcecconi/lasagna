# Payments Pipeline Scripts

This directory contains utility scripts for managing the payments pipeline databases and running data quality checks.

## 🚀 Quick Start

**Before running any scripts**, you need to access the workspace container:

```bash
# 1. Start the Lasagna stack (if not already running)
cd /path/to/lasagna
docker compose up -d

# 2. Access the workspace container
docker exec -it workspace bash

# 3. Navigate to the scripts directory
cd /usr/local/spark_dev/work/payments_pipeline_ingestion

# 4. Now you can run any script
python scripts/setup_payments_databases.py --with-sample-data
```

> **⚠️ Important**: All scripts must be run from inside the workspace container, not from your local machine.

## Available Scripts

### 1. `setup_payments_databases.py`

Recreates all payments databases from scratch with sample data.

**Usage:**
```bash
# From inside the workspace container:
# Set up complete pipeline with sample data
python scripts/setup_payments_databases.py --with-sample-data

# Set up only bronze layer
python scripts/setup_payments_databases.py --bronze-only

# Set up only silver layer (requires bronze to exist)
python scripts/setup_payments_databases.py --silver-only

# Set up complete pipeline without sample data
python scripts/setup_payments_databases.py
```

**Options:**
- `--with-sample-data`: Ingest sample data from the data generator
- `--bronze-only`: Only create bronze layer
- `--silver-only`: Only create silver layer (requires bronze to exist)

### 2. `cleanup_payments_databases.py`

Removes all payments databases and their tables.

**Usage:**
```bash
# From inside the workspace container:
# Clean up all payments databases
python scripts/cleanup_payments_databases.py

# Clean up with confirmation
python scripts/cleanup_payments_databases.py --force

# Clean up only bronze layer
python scripts/cleanup_payments_databases.py --bronze-only

# Clean up only silver layer
python scripts/cleanup_payments_databases.py --silver-only
```

**Options:**
- `--force`: Skip confirmation prompt
- `--bronze-only`: Only remove bronze layer
- `--silver-only`: Only remove silver layer

### 3. `run_data_quality.py`

Runs comprehensive data quality checks on the payments pipeline.

**Usage:**
```bash
# From inside the workspace container:
# Run data quality checks (text output)
python scripts/run_data_quality.py

# Run data quality checks (JSON output)
python scripts/run_data_quality.py --format json

# Save results to file
python scripts/run_data_quality.py --output results.txt

# Save JSON results to file
python scripts/run_data_quality.py --format json --output results.json
```

**Options:**
- `--format`: Output format (json or text, default: text)
- `--output`: Output file path (default: stdout)

## Common Workflows

### Complete Fresh Setup
```bash
# 1. Access the workspace container
docker exec -it workspace bash
cd /usr/local/spark_dev/work/payments_pipeline_ingestion

# 2. Clean up existing databases
python scripts/cleanup_payments_databases.py --force

# 3. Set up complete pipeline with sample data
python scripts/setup_payments_databases.py --with-sample-data

# 4. Run data quality checks
python scripts/run_data_quality.py
```

### Development Workflow
```bash
# 1. Access the workspace container
docker exec -it workspace bash
cd /usr/local/spark_dev/work/payments_pipeline_ingestion

# 2. Clean up and recreate
python scripts/cleanup_payments_databases.py --force
python scripts/setup_payments_databases.py --with-sample-data

# 3. Test changes
python scripts/run_data_quality.py

# 4. Generate more data if needed
cd ../payments_pipeline
python data_generator.py --days 30
```

### Troubleshooting
```bash
# Access the workspace container
docker exec -it workspace bash
cd /usr/local/spark_dev/work/payments_pipeline_ingestion

# Check data quality
python scripts/run_data_quality.py

# Recreate only silver layer
python scripts/cleanup_payments_databases.py --silver-only
python scripts/setup_payments_databases.py --silver-only

# Recreate only bronze layer
python scripts/cleanup_payments_databases.py --bronze-only
python scripts/setup_payments_databases.py --bronze-only
```

## Prerequisites

- Docker containers running (workspace, spark-master, etc.)
- Sample data files in `/usr/local/spark_dev/work/payments_pipeline/raw_data/` (for `--with-sample-data`)

## Running Scripts

**Important**: These scripts must be run from inside the workspace Docker container, not from your local machine.

### Accessing the Workspace Container

1. **Start the Lasagna stack**:
   ```bash
   cd /path/to/lasagna
   docker compose up -d
   ```

2. **Access the workspace container**:
   ```bash
   # Method 1: Direct container access
   docker exec -it workspace bash
   
   # Method 2: Via JupyterLab terminal
   # Open http://localhost:8888 in your browser
   # Click on Terminal in JupyterLab interface
   ```

3. **Navigate to the scripts directory**:
   ```bash
   cd /usr/local/spark_dev/work/payments_pipeline_ingestion
   ```

### Alternative: Using JupyterLab

You can also run these scripts from JupyterLab notebooks:
```python
# In a JupyterLab notebook cell
import subprocess
import sys

# Run a script from within the container
result = subprocess.run([
    sys.executable, 
    "/usr/local/spark_dev/work/payments_pipeline_ingestion/scripts/setup_payments_databases.py",
    "--with-sample-data"
], capture_output=True, text=True)

print(result.stdout)
if result.stderr:
    print("Errors:", result.stderr)
```

## Output

All scripts provide detailed console output showing:
- Progress indicators
- Success/failure status
- Error messages
- Summary statistics

## Error Handling

Scripts include comprehensive error handling:
- Graceful failure with clear error messages
- Rollback capabilities where applicable
- Detailed logging for troubleshooting

## Integration

These scripts are designed to work with:
- The existing payments pipeline codebase
- Docker containerized environment
- Jupyter notebooks for data exploration
- CI/CD pipelines for automated testing
