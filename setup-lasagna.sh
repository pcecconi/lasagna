#!/bin/bash

# LASAGNA Big Data Stack - Auto Memory Optimization Setup Script
# This script automatically detects available Docker memory and optimizes all configurations
# Author: LASAGNA Team
# Version: 1.0

set -e

# Ensure we're using bash with associative array support
if [ -z "$BASH_VERSION" ]; then
    echo "This script requires bash. Please run with: bash $0"
    exit 1
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/images"

# Memory tiers (in MB)
MEMORY_TIERS=(
    "2048:MINIMAL"      # 2GB - Minimal setup
    "4096:SMALL"        # 4GB - Small setup  
    "8192:MEDIUM"       # 8GB - Medium setup
    "16384:LARGE"       # 16GB - Large setup
    "32768:XLARGE"      # 32GB - Extra large setup
    "65536:HUGE"        # 64GB+ - Huge setup
)

# Configuration templates will be generated dynamically

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${PURPLE}================================${NC}"
    echo -e "${PURPLE}$1${NC}"
    echo -e "${PURPLE}================================${NC}"
}

# Function to detect available Docker memory
detect_docker_memory() {
    # Try to get Docker memory limit
    if command -v docker >/dev/null 2>&1; then
        # Check if Docker is running
        if ! docker info >/dev/null 2>&1; then
            echo "ERROR: Docker is not running" >&2
            exit 1
        fi
        
        # Get system memory info
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            TOTAL_MEMORY_MB=$(sysctl -n hw.memsize | awk '{print int($1/1024/1024)}')
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            # Linux
            TOTAL_MEMORY_MB=$(free -m | awk 'NR==2{print $2}')
        else
            echo "ERROR: Unsupported operating system: $OSTYPE" >&2
            exit 1
        fi
        
        # Estimate Docker memory (usually 75% of total system memory)
        DOCKER_MEMORY_MB=$((TOTAL_MEMORY_MB * 75 / 100))
        
        echo $DOCKER_MEMORY_MB
    else
        echo "ERROR: Docker is not installed or not in PATH" >&2
        exit 1
    fi
}

# Function to determine memory tier
determine_memory_tier() {
    local available_memory=$1
    local tier="MINIMAL"
    
    for tier_config in "${MEMORY_TIERS[@]}"; do
        local threshold=$(echo $tier_config | cut -d: -f1)
        local tier_name=$(echo $tier_config | cut -d: -f2)
        
        if [ $available_memory -ge $threshold ]; then
            tier=$tier_name
        else
            break
        fi
    done
    
    echo $tier
}

# Function to generate Trino configuration
generate_trino_config() {
    local tier=$1
    local available_memory=$2
    
    case $tier in
        "MINIMAL")
            cat > "$CONFIG_DIR/trino/conf/config.properties" << EOF
# Optimized Trino configuration for MINIMAL setup (${available_memory}MB)
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080

# Memory optimizations for limited RAM
query.max-memory=128MB
query.max-memory-per-node=64MB
query.max-total-memory=128MB

# Reduce parallelism to save memory
task.concurrency=1
task.max-worker-threads=2

# Optimize for small datasets
query.max-execution-time=5m
query.max-planning-time=2m

# Reduce buffer sizes
exchange.max-buffer-size=16MB
exchange.client-threads=2

# Catalog management
catalog.management=\${ENV:CATALOG_MANAGEMENT}
EOF

            cat > "$CONFIG_DIR/trino/conf/jvm.config" << EOF
-server
-agentpath:/usr/lib/trino/bin/libjvmkill.so

# Minimal memory settings for ${available_memory}MB system
-Xmx256m
-Xms128m

# G1GC settings optimized for very small heap
-XX:+UseG1GC
-XX:G1HeapRegionSize=4M
-XX:MaxGCPauseMillis=50

# Memory management
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow

# Minimal code cache
-XX:ReservedCodeCacheSize=32M

# Compilation optimizations
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000

# System settings
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=250000
-Dfile.encoding=UTF-8

# Allow loading dynamic agent used by JOL
-XX:+EnableDynamicAgentLoading
EOF
            ;;
        "SMALL")
            cat > "$CONFIG_DIR/trino/conf/config.properties" << EOF
# Optimized Trino configuration for SMALL setup (${available_memory}MB)
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080

# Memory optimizations for limited RAM
query.max-memory=256MB
query.max-memory-per-node=128MB
query.max-total-memory=256MB

# Reduce parallelism to save memory
task.concurrency=2
task.max-worker-threads=4

# Optimize for small datasets
query.max-execution-time=10m
query.max-planning-time=5m

# Reduce buffer sizes
exchange.max-buffer-size=32MB
exchange.client-threads=4

# Catalog management
catalog.management=\${ENV:CATALOG_MANAGEMENT}
EOF

            cat > "$CONFIG_DIR/trino/conf/jvm.config" << EOF
-server
-agentpath:/usr/lib/trino/bin/libjvmkill.so

# Small memory settings for ${available_memory}MB system
-Xmx512m
-Xms256m

# G1GC settings optimized for small heap
-XX:+UseG1GC
-XX:G1HeapRegionSize=8M
-XX:MaxGCPauseMillis=100

# Memory management
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow

# Minimal code cache
-XX:ReservedCodeCacheSize=64M

# Compilation optimizations
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000

# System settings
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=500000
-Dfile.encoding=UTF-8

# Allow loading dynamic agent used by JOL
-XX:+EnableDynamicAgentLoading
EOF
            ;;
        "MEDIUM")
            cat > "$CONFIG_DIR/trino/conf/config.properties" << EOF
# Optimized Trino configuration for MEDIUM setup (${available_memory}MB)
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080

# Memory optimizations for medium RAM
query.max-memory=1GB
query.max-memory-per-node=512MB
query.max-total-memory=1GB

# Moderate parallelism
task.concurrency=4
task.max-worker-threads=8

# Optimize for medium datasets
query.max-execution-time=15m
query.max-planning-time=10m

# Moderate buffer sizes
exchange.max-buffer-size=64MB
exchange.client-threads=8

# Catalog management
catalog.management=\${ENV:CATALOG_MANAGEMENT}
EOF

            cat > "$CONFIG_DIR/trino/conf/jvm.config" << EOF
-server
-agentpath:/usr/lib/trino/bin/libjvmkill.so

# Medium memory settings for ${available_memory}MB system
-Xmx2g
-Xms1g

# G1GC settings optimized for medium heap
-XX:+UseG1GC
-XX:G1HeapRegionSize=16M
-XX:MaxGCPauseMillis=200

# Memory management
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow

# Moderate code cache
-XX:ReservedCodeCacheSize=128M

# Compilation optimizations
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000

# System settings
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=1000000
-Dfile.encoding=UTF-8

# Allow loading dynamic agent used by JOL
-XX:+EnableDynamicAgentLoading
EOF
            ;;
        "LARGE"|"XLARGE"|"HUGE")
            cat > "$CONFIG_DIR/trino/conf/config.properties" << EOF
# Optimized Trino configuration for ${tier} setup (${available_memory}MB)
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080

# Memory optimizations for large RAM
query.max-memory=4GB
query.max-memory-per-node=2GB
query.max-total-memory=4GB

# High parallelism
task.concurrency=8
task.max-worker-threads=16

# Optimize for large datasets
query.max-execution-time=30m
query.max-planning-time=15m

# Large buffer sizes
exchange.max-buffer-size=128MB
exchange.client-threads=16

# Catalog management
catalog.management=\${ENV:CATALOG_MANAGEMENT}
EOF

            cat > "$CONFIG_DIR/trino/conf/jvm.config" << EOF
-server
-agentpath:/usr/lib/trino/bin/libjvmkill.so

# Large memory settings for ${available_memory}MB system
-Xmx8g
-Xms4g

# G1GC settings optimized for large heap
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:MaxGCPauseMillis=300

# Memory management
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow

# Large code cache
-XX:ReservedCodeCacheSize=256M

# Compilation optimizations
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000

# System settings
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-Dfile.encoding=UTF-8

# Allow loading dynamic agent used by JOL
-XX:+EnableDynamicAgentLoading
EOF
            ;;
    esac
}

# Function to generate Spark configuration
generate_spark_config() {
    local tier=$1
    local available_memory=$2
    
    case $tier in
        "MINIMAL")
            cat > "$CONFIG_DIR/workspace/conf/spark-defaults.conf" << EOF
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Configurações relativas ao MinIO e S3
spark.hadoop.fs.s3a.endpoint                                   http://minio:9000
spark.hadoop.fs.s3a.access.key                                 admin
spark.hadoop.fs.s3a.secret.key                                 password
spark.hadoop.fs.s3a.fast.upload                                true
spark.hadoop.fs.s3a.path.style.access                          true
spark.hadoop.fs.s3a.impl                                       org.apache.hadoop.fs.s3a.S3AFileSystem

# Configurações relativas ao Hive Metastore
spark.hadoop.javax.jdo.option.ConnectionDriverName             org.postgresql.Driver
spark.hadoop.javax.jdo.option.ConnectionURL                    jdbc:postgresql://postgres:5432/metastore_db
spark.hadoop.javax.jdo.option.ConnectionUserName               hive
spark.hadoop.javax.jdo.option.ConnectionPassword               hive123

spark.hadoop.datanucleus.schema.autoCreateAll                  true
spark.hadoop.datanucleus.schema.autoCreateTables               true
spark.hadoop.datanucleus.fixedDatastore                        false
spark.hadoop.hive.metastore.schema.verification                false
spark.hadoop.hive.metastore.schema.verification.record.version false

spark.sql.warehouse.dir                                        s3a://warehouse/
spark.sql.catalogImplementation                                hive
spark.sql.hive.metastore.version                               3.0.0
spark.sql.hive.metastore.uris                                  thrift://hive-metastore:9083
spark.sql.hive.metastore.jars                                  /usr/local/lib/python3.10/dist-packages/pyspark/hms-3.0.0/jars/*
spark.sql.files.ignoreMissingFiles                             true

# Configurando Cluster
spark.master                                                   spark://spark-master:7077
spark.executor.instances                                       1

# Memory optimizations for MINIMAL setup (${available_memory}MB)
spark.driver.memory                                            128m
spark.driver.maxResultSize                                     64m
spark.executor.memory                                          128m
spark.executor.memoryFraction                                   0.4
spark.executor.cores                                            1
spark.sql.adaptive.enabled                                     true
spark.sql.adaptive.coalescePartitions.enabled                 true
spark.sql.adaptive.skewJoin.enabled                            true
EOF
            ;;
        "SMALL")
            cat > "$CONFIG_DIR/workspace/conf/spark-defaults.conf" << EOF
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Configurações relativas ao MinIO e S3
spark.hadoop.fs.s3a.endpoint                                   http://minio:9000
spark.hadoop.fs.s3a.access.key                                 admin
spark.hadoop.fs.s3a.secret.key                                 password
spark.hadoop.fs.s3a.fast.upload                                true
spark.hadoop.fs.s3a.path.style.access                          true
spark.hadoop.fs.s3a.impl                                       org.apache.hadoop.fs.s3a.S3AFileSystem

# Configurações relativas ao Hive Metastore
spark.hadoop.javax.jdo.option.ConnectionDriverName             org.postgresql.Driver
spark.hadoop.javax.jdo.option.ConnectionURL                    jdbc:postgresql://postgres:5432/metastore_db
spark.hadoop.javax.jdo.option.ConnectionUserName               hive
spark.hadoop.javax.jdo.option.ConnectionPassword               hive123

spark.hadoop.datanucleus.schema.autoCreateAll                  true
spark.hadoop.datanucleus.schema.autoCreateTables               true
spark.hadoop.datanucleus.fixedDatastore                        false
spark.hadoop.hive.metastore.schema.verification                false
spark.hadoop.hive.metastore.schema.verification.record.version false

spark.sql.warehouse.dir                                        s3a://warehouse/
spark.sql.catalogImplementation                                hive
spark.sql.hive.metastore.version                               3.0.0
spark.sql.hive.metastore.uris                                  thrift://hive-metastore:9083
spark.sql.hive.metastore.jars                                  /usr/local/lib/python3.10/dist-packages/pyspark/hms-3.0.0/jars/*
spark.sql.files.ignoreMissingFiles                             true

# Configurando Cluster
spark.master                                                   spark://spark-master:7077
spark.executor.instances                                       2

# Memory optimizations for SMALL setup (${available_memory}MB)
spark.driver.memory                                            256m
spark.driver.maxResultSize                                     128m
spark.executor.memory                                          256m
spark.executor.memoryFraction                                   0.5
spark.executor.cores                                            1
spark.sql.adaptive.enabled                                     true
spark.sql.adaptive.coalescePartitions.enabled                 true
spark.sql.adaptive.skewJoin.enabled                            true
EOF
            ;;
        "MEDIUM")
            cat > "$CONFIG_DIR/workspace/conf/spark-defaults.conf" << EOF
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Configurações relativas ao MinIO e S3
spark.hadoop.fs.s3a.endpoint                                   http://minio:9000
spark.hadoop.fs.s3a.access.key                                 admin
spark.hadoop.fs.s3a.secret.key                                 password
spark.hadoop.fs.s3a.fast.upload                                true
spark.hadoop.fs.s3a.path.style.access                          true
spark.hadoop.fs.s3a.impl                                       org.apache.hadoop.fs.s3a.S3AFileSystem

# Configurações relativas ao Hive Metastore
spark.hadoop.javax.jdo.option.ConnectionDriverName             org.postgresql.Driver
spark.hadoop.javax.jdo.option.ConnectionURL                    jdbc:postgresql://postgres:5432/metastore_db
spark.hadoop.javax.jdo.option.ConnectionUserName               hive
spark.hadoop.javax.jdo.option.ConnectionPassword               hive123

spark.hadoop.datanucleus.schema.autoCreateAll                  true
spark.hadoop.datanucleus.schema.autoCreateTables               true
spark.hadoop.datanucleus.fixedDatastore                        false
spark.hadoop.hive.metastore.schema.verification                false
spark.hadoop.hive.metastore.schema.verification.record.version false

spark.sql.warehouse.dir                                        s3a://warehouse/
spark.sql.catalogImplementation                                hive
spark.sql.hive.metastore.version                               3.0.0
spark.sql.hive.metastore.uris                                  thrift://hive-metastore:9083
spark.sql.hive.metastore.jars                                  /usr/local/lib/python3.10/dist-packages/pyspark/hms-3.0.0/jars/*
spark.sql.files.ignoreMissingFiles                             true

# Configurando Cluster
spark.master                                                   spark://spark-master:7077
spark.executor.instances                                       2

# Memory optimizations for MEDIUM setup (${available_memory}MB)
spark.driver.memory                                            1g
spark.driver.maxResultSize                                     512m
spark.executor.memory                                          1g
spark.executor.memoryFraction                                   0.6
spark.executor.cores                                            2
spark.sql.adaptive.enabled                                     true
spark.sql.adaptive.coalescePartitions.enabled                 true
spark.sql.adaptive.skewJoin.enabled                            true
EOF
            ;;
        "LARGE"|"XLARGE"|"HUGE")
            cat > "$CONFIG_DIR/workspace/conf/spark-defaults.conf" << EOF
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Configurações relativas ao MinIO e S3
spark.hadoop.fs.s3a.endpoint                                   http://minio:9000
spark.hadoop.fs.s3a.access.key                                 admin
spark.hadoop.fs.s3a.secret.key                                 password
spark.hadoop.fs.s3a.fast.upload                                true
spark.hadoop.fs.s3a.path.style.access                          true
spark.hadoop.fs.s3a.impl                                       org.apache.hadoop.fs.s3a.S3AFileSystem

# Configurações relativas ao Hive Metastore
spark.hadoop.javax.jdo.option.ConnectionDriverName             org.postgresql.Driver
spark.hadoop.javax.jdo.option.ConnectionURL                    jdbc:postgresql://postgres:5432/metastore_db
spark.hadoop.javax.jdo.option.ConnectionUserName               hive
spark.hadoop.javax.jdo.option.ConnectionPassword               hive123

spark.hadoop.datanucleus.schema.autoCreateAll                  true
spark.hadoop.datanucleus.schema.autoCreateTables               true
spark.hadoop.datanucleus.fixedDatastore                        false
spark.hadoop.hive.metastore.schema.verification                false
spark.hadoop.hive.metastore.schema.verification.record.version false

spark.sql.warehouse.dir                                        s3a://warehouse/
spark.sql.catalogImplementation                                hive
spark.sql.hive.metastore.version                               3.0.0
spark.sql.hive.metastore.uris                                  thrift://hive-metastore:9083
spark.sql.hive.metastore.jars                                  /usr/local/lib/python3.10/dist-packages/pyspark/hms-3.0.0/jars/*
spark.sql.files.ignoreMissingFiles                             true

# Configurando Cluster
spark.master                                                   spark://spark-master:7077
spark.executor.instances                                       4

# Memory optimizations for ${tier} setup (${available_memory}MB)
spark.driver.memory                                            2g
spark.driver.maxResultSize                                     1g
spark.executor.memory                                          2g
spark.executor.memoryFraction                                   0.7
spark.executor.cores                                            4
spark.sql.adaptive.enabled                                     true
spark.sql.adaptive.coalescePartitions.enabled                 true
spark.sql.adaptive.skewJoin.enabled                            true
EOF
            ;;
    esac
}

# Function to generate Docker Compose configuration
generate_docker_compose_config() {
    local tier=$1
    local available_memory=$2
    
    # Calculate memory limits based on tier
    local trino_memory_limit
    local trino_memory_reservation
    
    case $tier in
        "MINIMAL")
            trino_memory_limit="384M"
            trino_memory_reservation="256M"
            ;;
        "SMALL")
            trino_memory_limit="768M"
            trino_memory_reservation="512M"
            ;;
        "MEDIUM")
            trino_memory_limit="2G"
            trino_memory_reservation="1G"
            ;;
        "LARGE"|"XLARGE"|"HUGE")
            trino_memory_limit="8G"
            trino_memory_reservation="4G"
            ;;
    esac
    
    # Backup original docker-compose.yml
    cp "$SCRIPT_DIR/docker-compose.yml" "$SCRIPT_DIR/docker-compose.yml.backup"
    
    # Update docker-compose.yml with memory limits
    sed -i.bak "s/memory: 768M/memory: $trino_memory_limit/g" "$SCRIPT_DIR/docker-compose.yml"
    sed -i.bak "s/memory: 512M/memory: $trino_memory_reservation/g" "$SCRIPT_DIR/docker-compose.yml"
    
    # Clean up backup file
    rm -f "$SCRIPT_DIR/docker-compose.yml.bak"
}

# Function to display configuration summary
display_config_summary() {
    local tier=$1
    local available_memory=$2
    
    print_header "Configuration Summary"
    echo -e "${CYAN}Memory Tier:${NC} $tier"
    echo -e "${CYAN}Available Memory:${NC} ${available_memory}MB"
    echo ""
    echo -e "${CYAN}Trino Configuration:${NC}"
    case $tier in
        "MINIMAL")
            echo "  - Heap Size: 256MB"
            echo "  - Query Memory: 128MB"
            echo "  - Docker Limit: 384MB"
            ;;
        "SMALL")
            echo "  - Heap Size: 512MB"
            echo "  - Query Memory: 256MB"
            echo "  - Docker Limit: 768MB"
            ;;
        "MEDIUM")
            echo "  - Heap Size: 2GB"
            echo "  - Query Memory: 1GB"
            echo "  - Docker Limit: 2GB"
            ;;
        "LARGE"|"XLARGE"|"HUGE")
            echo "  - Heap Size: 8GB"
            echo "  - Query Memory: 4GB"
            echo "  - Docker Limit: 8GB"
            ;;
    esac
    
    echo ""
    echo -e "${CYAN}Spark Configuration:${NC}"
    case $tier in
        "MINIMAL")
            echo "  - Driver Memory: 128MB"
            echo "  - Executor Memory: 128MB"
            echo "  - Executor Instances: 1"
            ;;
        "SMALL")
            echo "  - Driver Memory: 256MB"
            echo "  - Executor Memory: 256MB"
            echo "  - Executor Instances: 2"
            ;;
        "MEDIUM")
            echo "  - Driver Memory: 1GB"
            echo "  - Executor Memory: 1GB"
            echo "  - Executor Instances: 2"
            ;;
        "LARGE"|"XLARGE"|"HUGE")
            echo "  - Driver Memory: 2GB"
            echo "  - Executor Memory: 2GB"
            echo "  - Executor Instances: 4"
            ;;
    esac
}

# Function to validate configuration
validate_configuration() {
    print_status "Validating configuration files..."
    
    # Check if Trino config files exist
    if [ ! -f "$CONFIG_DIR/trino/conf/config.properties" ]; then
        print_error "Trino config.properties not found"
        return 1
    fi
    
    if [ ! -f "$CONFIG_DIR/trino/conf/jvm.config" ]; then
        print_error "Trino jvm.config not found"
        return 1
    fi
    
    # Check if Spark config file exists
    if [ ! -f "$CONFIG_DIR/workspace/conf/spark-defaults.conf" ]; then
        print_error "Spark spark-defaults.conf not found"
        return 1
    fi
    
    # Check if docker-compose.yml exists
    if [ ! -f "$SCRIPT_DIR/docker-compose.yml" ]; then
        print_error "docker-compose.yml not found"
        return 1
    fi
    
    print_success "All configuration files are present"
    return 0
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -m, --memory SIZE       Override detected memory (in MB)"
    echo "  -t, --tier TIER         Override detected tier (MINIMAL|SMALL|MEDIUM|LARGE|XLARGE|HUGE)"
    echo "  -f, --force             Force regeneration of configurations"
    echo "  -v, --verbose           Enable verbose output"
    echo ""
    echo "Examples:"
    echo "  $0                      # Auto-detect and configure"
    echo "  $0 -m 4096              # Configure for 4GB memory"
    echo "  $0 -t MEDIUM            # Configure for medium tier"
    echo "  $0 -f                   # Force regeneration"
}

# Main function
main() {
    local override_memory=""
    local override_tier=""
    local force_regenerate=false
    local verbose=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_usage
                exit 0
                ;;
            -m|--memory)
                override_memory="$2"
                shift 2
                ;;
            -t|--tier)
                override_tier="$2"
                shift 2
                ;;
            -f|--force)
                force_regenerate=true
                shift
                ;;
            -v|--verbose)
                verbose=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    print_header "LASAGNA Big Data Stack - Auto Memory Optimization"
    
    # Detect or use override memory
    if [ -n "$override_memory" ]; then
        available_memory=$override_memory
        print_status "Using override memory: ${available_memory}MB"
    else
        print_status "Detecting available Docker memory..."
        available_memory=$(detect_docker_memory)
        print_success "Detected Docker memory: ${available_memory}MB"
    fi
    
    # Determine or use override tier
    if [ -n "$override_tier" ]; then
        tier=$override_tier
        print_status "Using override tier: $tier"
    else
        tier=$(determine_memory_tier $available_memory)
        print_status "Detected memory tier: $tier"
    fi
    
    # Check if we need to regenerate configurations
    if [ "$force_regenerate" = false ]; then
        if validate_configuration; then
            print_warning "Configurations already exist. Use -f to force regeneration."
            read -p "Do you want to continue? (y/N): " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                print_status "Configuration cancelled by user"
                exit 0
            fi
        fi
    fi
    
    # Generate configurations
    print_status "Generating Trino configuration for $tier tier..."
    generate_trino_config $tier $available_memory
    
    print_status "Generating Spark configuration for $tier tier..."
    generate_spark_config $tier $available_memory
    
    print_status "Updating Docker Compose configuration..."
    generate_docker_compose_config $tier $available_memory
    
    # Display summary
    display_config_summary $tier $available_memory
    
    print_success "Configuration complete!"
    print_status "You can now run: docker-compose up -d"
    print_status "To rebuild images with new configs: docker-compose build"
}

# Run main function
main "$@"
