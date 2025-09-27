# LASAGNA Big Data Stack - Auto Memory Optimization

This script automatically detects your system's available Docker memory and optimizes all LASAGNA stack configurations for optimal performance.

## 🚀 Quick Start

```bash
# Auto-detect memory and configure
./setup-lasagna.sh

# Start the stack
docker-compose up -d
```

## 📋 Features

- **Automatic Memory Detection**: Detects available Docker memory on macOS and Linux
- **Tiered Configurations**: 3 memory tiers from SMALL (4GB) to LARGE (16GB+)
- **Complete Optimization**: Configures Trino, Spark, and Docker Compose
- **Smart Defaults**: Uses 75% of system memory for Docker allocation
- **Override Options**: Manual memory/tier specification
- **Validation**: Checks all configuration files before proceeding
- **Automatic Backup**: Creates timestamped backups of existing configurations before overwriting

## 🎯 Memory Tiers

> **Note**: The SMALL tier preserves your exact current configuration settings, ensuring compatibility with existing setups.

| Tier | Memory Range | Trino Heap | Query Memory | Spark Driver | Executor Memory | Use Case |
|------|-------------|------------|--------------|---------------|-----------------|----------|
| SMALL | 4-8GB | 2GB | 512MB | 2GB | 512MB | Development, Small Datasets |
| MEDIUM | 8-16GB | 4GB | 1GB | 4GB | 1GB | Medium Workloads |
| LARGE | 16GB+ | 8GB | 4GB | 8GB | 2GB | Production, Large Datasets |

## 🛠️ Usage Options

```bash
# Auto-detect and configure
./setup-lasagna.sh

# Specify memory amount (in MB)
./setup-lasagna.sh -m 4096

# Specify tier directly
./setup-lasagna.sh -t MEDIUM

# Force regeneration of configs
./setup-lasagna.sh -f

# Verbose output
./setup-lasagna.sh -v

# Show help
./setup-lasagna.sh -h
```

## 📁 What It Configures

### Trino Configuration
- **Memory Settings**: Heap size, query memory limits
- **JVM Settings**: G1GC tuning, code cache size
- **Performance**: Task concurrency, buffer sizes
- **Docker Limits**: Memory limits and reservations

### Spark Configuration
- **Complete Memory Settings**: All Spark memory configurations included (driver, executor, cores)
- **Driver Memory**: Optimized for each tier (2GB-8GB)
- **Executor Memory**: Balanced across workers (512MB-2GB)
- **Executor Cores**: 2-8 cores per executor
- **Dynamic Allocation**: 1-16 executors based on tier
- **Adaptive Query**: Enabled for better performance
- **Resource Management**: Complete memory and core configuration

### Docker Compose
- **Memory Limits**: Prevents OOM kills
- **Resource Reservations**: Ensures stable startup
- **Service Dependencies**: Proper startup order

## 🔧 Manual Override Examples

```bash
# For a 6GB system (SMALL tier)
./setup-lasagna.sh -m 6144

# Force SMALL tier for testing
./setup-lasagna.sh -t SMALL

# Override for specific hardware
./setup-lasagna.sh -m 12288 -t MEDIUM
```

## 🚨 Troubleshooting

### Memory Issues
- **OOM Errors**: Run with `-t SMALL` for testing
- **Slow Performance**: Try `-t MEDIUM` or `-t LARGE`
- **Docker Not Starting**: Check Docker Desktop memory settings

### Configuration Issues
- **Config Not Applied**: Use `-f` to force regeneration
- **Wrong Tier**: Use `-t` to override detected tier
- **Validation Errors**: Check file permissions and paths

## 📊 Performance Expectations

| Tier | Concurrent Queries | Dataset Size | Query Complexity |
|------|-------------------|--------------|------------------|
| SMALL | 2-4 | <5GB | Basic analytics |
| MEDIUM | 4-8 | <20GB | Complex joins |
| LARGE | 8+ | 20GB+ | Full analytics |

## 🔄 Workflow

1. **Detect Memory**: System memory → Docker allocation
2. **Determine Tier**: Map memory to appropriate tier
3. **Backup Existing**: Create timestamped backups of current configs
4. **Generate Configs**: Create optimized configurations
5. **Validate**: Check all files are present
6. **Summary**: Display configuration details
7. **Ready**: Stack ready to start

## 💾 Automatic Backup System

The script automatically creates backups of existing configuration files before overwriting them:

- **Backup Format**: `filename.YYYYMMDD_HHMMSS.backup`
- **Backup Location**: Same directory as original files
- **Files Backed Up**:
  - `images/trino/conf/config.properties`
  - `images/trino/conf/jvm.config`
  - `images/workspace/conf/spark-defaults.conf`
  - `docker-compose.yml`

**Example Backup Output**:
```
[INFO] Backed up existing configuration: config.properties -> config.properties.20250927_111518.backup
[INFO] Backed up existing configuration: jvm.config -> jvm.config.20250927_111518.backup
```

**Restoring Backups**: Simply copy the backup file back to its original name:
```bash
cp images/trino/conf/config.properties.20250927_111518.backup images/trino/conf/config.properties
```

## 💡 Tips

- **First Run**: Always use auto-detection first
- **Testing**: Use SMALL tier for development
- **Production**: Use MEDIUM+ for real workloads
- **Monitoring**: Watch `docker stats` for memory usage
- **Scaling**: Re-run script if you change hardware

## 🆘 Support

If you encounter issues:
1. Check Docker is running: `docker info`
2. Verify memory detection: `./setup-lasagna.sh -v`
3. Try minimal config: `./setup-lasagna.sh -t SMALL`
4. Check logs: `docker-compose logs`

---

**Happy Data Processing! 🎉**
