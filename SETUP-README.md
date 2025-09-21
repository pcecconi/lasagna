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
- **Tiered Configurations**: 6 memory tiers from MINIMAL (2GB) to HUGE (64GB+)
- **Complete Optimization**: Configures Trino, Spark, and Docker Compose
- **Smart Defaults**: Uses 75% of system memory for Docker allocation
- **Override Options**: Manual memory/tier specification
- **Validation**: Checks all configuration files before proceeding

## 🎯 Memory Tiers

| Tier | Memory Range | Trino Heap | Spark Driver | Use Case |
|------|-------------|------------|---------------|----------|
| MINIMAL | 2-4GB | 256MB | 128MB | Development, Testing |
| SMALL | 4-8GB | 512MB | 256MB | Small Datasets |
| MEDIUM | 8-16GB | 2GB | 1GB | Medium Workloads |
| LARGE | 16-32GB | 8GB | 2GB | Production-like |
| XLARGE | 32-64GB | 8GB | 2GB | Large Datasets |
| HUGE | 64GB+ | 8GB | 2GB | Enterprise Scale |

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
- **Driver Memory**: Optimized for each tier
- **Executor Memory**: Balanced across workers
- **Adaptive Query**: Enabled for better performance
- **Resource Allocation**: Cores and instances per tier

### Docker Compose
- **Memory Limits**: Prevents OOM kills
- **Resource Reservations**: Ensures stable startup
- **Service Dependencies**: Proper startup order

## 🔧 Manual Override Examples

```bash
# For a 6GB system (between SMALL and MEDIUM)
./setup-lasagna.sh -m 6144

# Force MINIMAL tier for testing
./setup-lasagna.sh -t MINIMAL

# Override for specific hardware
./setup-lasagna.sh -m 12288 -t MEDIUM
```

## 🚨 Troubleshooting

### Memory Issues
- **OOM Errors**: Run with `-t MINIMAL` for testing
- **Slow Performance**: Try `-t MEDIUM` or `-t LARGE`
- **Docker Not Starting**: Check Docker Desktop memory settings

### Configuration Issues
- **Config Not Applied**: Use `-f` to force regeneration
- **Wrong Tier**: Use `-t` to override detected tier
- **Validation Errors**: Check file permissions and paths

## 📊 Performance Expectations

| Tier | Concurrent Queries | Dataset Size | Query Complexity |
|------|-------------------|--------------|------------------|
| MINIMAL | 1-2 | <1GB | Simple aggregations |
| SMALL | 2-4 | <5GB | Basic analytics |
| MEDIUM | 4-8 | <20GB | Complex joins |
| LARGE+ | 8+ | 20GB+ | Full analytics |

## 🔄 Workflow

1. **Detect Memory**: System memory → Docker allocation
2. **Determine Tier**: Map memory to appropriate tier
3. **Generate Configs**: Create optimized configurations
4. **Validate**: Check all files are present
5. **Summary**: Display configuration details
6. **Ready**: Stack ready to start

## 💡 Tips

- **First Run**: Always use auto-detection first
- **Testing**: Use MINIMAL tier for development
- **Production**: Use MEDIUM+ for real workloads
- **Monitoring**: Watch `docker stats` for memory usage
- **Scaling**: Re-run script if you change hardware

## 🆘 Support

If you encounter issues:
1. Check Docker is running: `docker info`
2. Verify memory detection: `./setup-lasagna.sh -v`
3. Try minimal config: `./setup-lasagna.sh -t MINIMAL`
4. Check logs: `docker-compose logs`

---

**Happy Data Processing! 🎉**
