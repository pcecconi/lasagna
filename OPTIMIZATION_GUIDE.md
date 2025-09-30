# 🚀 Lasagna System Optimization Guide

## 📊 Current System Analysis

**Your System:**
- **RAM**: 15GB total, only **1.2GB available** (very tight!)
- **CPU**: 8 cores
- **Current Memory Usage**: 14GB/15GB (93% used)

**Current Issues:**
- ✅ Executor crashes due to memory pressure
- ✅ System struggling to keep up
- ✅ Aggressive memory allocation

## 🔧 Optimization Strategy

### 1. **Docker Memory Optimization**

**Before (Current):**
```
- spark-worker-a: 3GB
- spark-worker-b: 3GB  
- workspace: 4GB
- trino: 2G
- Other services: ~1GB
Total: ~13GB allocated
```

**After (Optimized):**
```
- spark-worker-a: 2.5GB (only 1 worker)
- workspace: 3GB
- trino: 1GB
- Other services: ~1GB
Total: ~7.5GB allocated
```

### 2. **Spark Configuration Optimization**

**Key Changes:**
- **Driver Memory**: 1GB (was unlimited)
- **Executor Memory**: 1GB (was unlimited)
- **Parallelism**: 4 (reduced from default)
- **Shuffle Partitions**: 8 (reduced from 200)
- **Dynamic Allocation**: Enabled with conservative limits
- **GC Optimization**: G1GC with optimized settings

### 3. **Architecture Changes**

**Before:**
- 2 Spark workers × 3GB = 6GB
- Aggressive parallelism
- No memory limits

**After:**
- 1 Spark worker × 2.5GB = 2.5GB
- Conservative parallelism
- Strict memory limits

## 🛠️ How to Apply Optimizations

### Option 1: Automatic (Recommended)

```bash
# Apply optimized configuration
python optimize_system.py optimize

# Restart containers
docker compose down
docker compose up -d
```

### Option 2: Manual

```bash
# 1. Backup current configuration
cp docker-compose.yml docker-compose.yml.backup

# 2. Apply optimized configuration
cp docker-compose.optimized.yml docker-compose.yml

# 3. Update Spark utils
cp work/payments_pipeline_ingestion/src/payments_pipeline/utils/spark_optimized.py \
   work/payments_pipeline_ingestion/src/payments_pipeline/utils/spark.py

# 4. Restart containers
docker compose down
docker compose up -d
```

### Option 3: Restore Original Configuration

```bash
# Restore original configuration
python optimize_system.py restore

# Restart containers
docker compose down
docker compose up -d
```

## 📈 Expected Improvements

### Memory Usage
- **Before**: ~13GB allocated, system struggling
- **After**: ~7.5GB allocated, 7.5GB free for system

### Stability
- **Before**: Executor crashes, OOM errors
- **After**: Stable execution, graceful memory management

### Performance
- **Before**: System swapping, slow response
- **After**: Faster response, no swapping

## 🔍 Monitoring

### Check System Status
```bash
# Check memory usage
free -h

# Check Docker stats
docker stats --no-stream

# Check Spark UI
# http://localhost:5050 (Spark Master)
# http://localhost:5051 (Spark Worker)
```

### Monitor Spark Jobs
```bash
# Check Spark logs
docker logs spark-worker-a
docker logs spark-master
docker logs workspace
```

## ⚠️ Important Notes

1. **First Run**: The optimized configuration will be slower initially as it's more conservative
2. **Memory**: You'll have much more free memory (7.5GB vs 1.2GB)
3. **Workers**: Only 1 worker instead of 2 (still sufficient for most workloads)
4. **Parallelism**: Reduced parallelism but more stable execution

## 🚨 Troubleshooting

### If Jobs Still Fail:
1. **Check memory**: `free -h` should show >5GB available
2. **Check logs**: `docker logs spark-worker-a`
3. **Reduce further**: Lower `spark.executor.memory` to 512m

### If Too Slow:
1. **Increase parallelism**: Set `spark.default.parallelism` to 6
2. **Add worker**: Uncomment `spark-worker-b` in docker-compose.yml
3. **Increase memory**: Set `spark.executor.memory` to 1.5g

## 📋 Configuration Files

- `docker-compose.optimized.yml` - Optimized Docker configuration
- `work/payments_pipeline_ingestion/src/payments_pipeline/utils/spark_optimized.py` - Optimized Spark settings
- `optimize_system.py` - Automation script

## 🎯 Next Steps

1. **Apply optimizations** using the script
2. **Test with a small job** to verify stability
3. **Monitor system resources** during execution
4. **Adjust if needed** based on performance

The optimized configuration should eliminate executor crashes and make your system much more stable!
