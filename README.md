![alt text](docs/pastabricks-2.png "Lasagna")
Lasagna (or _pastabricks_) is a interactive development environment built by [Gabriel Marques](https://github.com/gmrqs) originally [here](https://github.com/gmrqs/lasagna) to learn and practice PySpark.

It's built using Docker Compose template, provisioning a Jupyter Lab, a two-workers Spark Standalone Cluster, MinIO Object Storage, a Hive Standalone Metastore, Trino and a Kafka cluster for simulating events. 

## Prerequisites
- Docker Desktop
- Docker Compose
- Bash shell (for setup scripts)

## Quick Start

### Option 1: Automated Setup (Recommended)
For the best experience, use the automated setup script that configures optimal settings based on your system's available memory:

```bash
# Clone the repository
git clone <repository-url>
cd lasagna

# Run the automated setup script
bash setup-lasagna.sh

# Start the stack
docker compose up -d
```

### Option 2: Manual Setup
If you prefer manual configuration:

```bash
# Clone the repository
git clone <repository-url>
cd lasagna

# Start the stack
docker compose up -d
```

<sub><sup>Docker will build the images automatically. We recommend having a wired internet connection for the initial setup.</sub></sup>

After all container are up and running, execute the following to get Jupyter Lab access link: 

```bash
 docker logs workspace 2>&1 | grep http://127.0.0.1
```

<sub><sup>(you can also the the link in docker desktop logs)</sup></sub>

Click on the link _http://127.0.0.1:8888/lab?token=<token_gigante_super_seguro>_

To start the Kafka broker you need to go to the kafka folder and execute the following:

```bash
docker compose up -d
```

## Setup Scripts and Tools

### 🚀 Automated Setup Script (`setup-lasagna.sh`)

The `setup-lasagna.sh` script provides intelligent configuration based on your system's available memory, ensuring optimal performance and stability.

#### Features:
- **Memory Detection**: Automatically detects available Docker memory
- **Tiered Configuration**: Provides 6 memory tiers (2GB to 64GB+)
- **Stable Settings**: Focuses on reliability over aggressive optimization
- **Backup Protection**: Creates timestamped backups of existing configurations
- **Comprehensive Configuration**: Sets up Spark, Trino, and JVM settings

#### Usage:
```bash
# Run the setup script
bash setup-lasagna.sh

# The script will:
# 1. Detect your available memory
# 2. Select appropriate configuration tier
# 3. Backup existing configurations
# 4. Apply optimized settings
# 5. Display configuration summary
```

#### Memory Tiers:
- **MINIMAL (2GB)**: Basic functionality, minimal resources
- **SMALL (4GB)**: Light workloads, single-user development
- **MEDIUM (8GB)**: Standard development, moderate workloads
- **LARGE (16GB)**: Heavy workloads, multiple users
- **XLARGE (32GB)**: Enterprise-level workloads
- **HUGE (64GB+)**: Maximum performance configuration

### 🔍 Validation Script (`validate-setup.sh`)

The `validate-setup.sh` script comprehensively tests all LASAGNA components to ensure everything is working correctly.

#### Features:
- **Service Health Checks**: Verifies all containers are running
- **Connectivity Tests**: Tests connections between all services
- **Table Format Testing**: Validates Hive, Delta Lake, and Iceberg functionality
- **Performance Benchmarks**: Runs basic performance tests
- **Configuration Validation**: Checks Spark and Trino configurations
- **Detailed Reporting**: Provides comprehensive status reports

#### Usage:
```bash
# Run validation after setup
bash validate-setup.sh

# The script will test:
# ✅ Container status
# ✅ Service connectivity
# ✅ Table operations
# ✅ Cross-engine querying
# ✅ Performance metrics
```

### 📊 Architecture Showcase Notebook (`work/projects/architecture_showcase.ipynb`)

A comprehensive Jupyter notebook that demonstrates and validates all LASAGNA components through hands-on examples.

#### What It Demonstrates:

**1. Environment Setup**
- Service connection testing
- Sample data generation
- Spark session configuration

**2. Table Format Testing**
- **Hive Tables**: Traditional data warehouse functionality
- **Delta Lake**: ACID transactions, time travel, and data versioning
- **Apache Iceberg**: Schema evolution and advanced partitioning

**3. Cross-Engine Querying**
- Trino integration with all table formats
- Cross-catalog querying capabilities
- SQL magic commands demonstration

**4. Performance Analysis**
- Query performance comparisons across formats
- Benchmarking different workloads
- Resource utilization monitoring

**5. Advanced Features**
- ACID transaction demonstrations
- Time travel capabilities
- Schema evolution examples
- Partitioning strategies

#### Usage:
1. Access JupyterLab at `http://127.0.0.1:8888/lab`
2. Navigate to `work/projects/architecture_showcase.ipynb`
3. Run all cells to see the complete demonstration
4. Modify examples to explore different scenarios

#### Sample Data:
The notebook generates realistic sample datasets:
- **Employee Data**: 10,000 records with departments, salaries, locations
- **Sales Data**: 50,000 records with products, regions, transactions

### What does Lasagna creates?

![alt text](docs/analytics-lab.png "Title")

The `docker-compose.yml` template create a series of containers:

#### :orange_book: Workspace
A Jupyter Lab client for interactive development sessions, featuring:
+ A _work_ directory in order to persists your scripts and notebooks;
+ `spark-defaults.conf` pre-configured to make Spark Sessions easier to create;
+ Dedicated kernels for PySpark with Hive, Iceberg or Delta;

> :eyes: Use `%SparkSession` command to easily configure Spark Session

![alt text](docs/kernels.gif "Title")
+ [jupyter_sql_editor](https://github.com/CybercentreCanada/jupyterlab-sql-editor) extension for SQL execution with `%sparksql` and `%trino` magic commands;
+ [jupyterlab_s3_browser](https://github.com/IBM/jupyterlab-s3-browser) extension to easily browse MinIO S3 buckets;

#### :open_file_folder: MinIO Object Storage
A single MinIO instance to serve as object storage:
+ Web UI accessible at localhost:9090 (user: `admin` password: `password`)
+ s3a protocol API available at port 9000;
+ _mount/minio_ and _mount/minio-config_ directories mounted to persist data between sessions.

#### :sparkles: Spark Cluster
A standalone spark cluster for workload processing:
+ 1 Master node (master at port 7077, web-ui at localhost:5050)
+ 2 Worker nodes (web-ui at localhost:5051 and localhost:5052)
+ All the necessary dependencies for MinIO connection;
+ Connectivity with MinIO @ port 9000.

#### :honeybee: Hive Standalone Metastore
A Hive Standalone Metastore instance using PostgreSQL as back-end database allowinto to persist table metadata between sessions.
+ _mount/postgres_ directory to persist tables between development sessions;
+ Connectivity with Spark cluster at through  thift protocol at port 9083;
+ Connectivity with PostgresSQL through JDBC at port 5432.

#### :rabbit: Trino
A single Trino instace to serve as query engine.
+ Hive, Delta e Iceberg catalos configured. All tables created in using PySpark are accessible with Trino;
+ Standar service available at port 8080.

> :eyes: Don't forget you can use the `%trino` magic command in your notebooks!

#### :ocean: Kafka
A separate docker compose template with a zookeper + kafka single-node instance to mock data-streams with a python producer.
+ Uses the same network as the lasagna docker compose creates;
+ A kafka-producer notebook/script is available to create random events with Faker library;
+ Accessible at kafka:29092.

## Troubleshooting

### Common Issues and Solutions

#### 🔧 Service Connectivity Issues
If you're experiencing connection problems between services:

```bash
# Run the validation script to diagnose issues
bash validate-setup.sh

# Check container status
docker ps

# View service logs
docker logs <container-name>
```

#### 🚀 Performance Issues
If LASAGNA is running slowly or consuming too much memory:

```bash
# Re-run setup with different memory tier
bash setup-lasagna.sh

# Check Docker resource allocation
docker stats
```

#### 📊 Table Format Problems
If you're having issues with Delta Lake or Iceberg tables:

1. **Run the Architecture Showcase**: Execute `work/projects/architecture_showcase.ipynb` to test all table formats
2. **Check Spark Configuration**: Verify Spark session includes required extensions
3. **Validate Storage**: Ensure MinIO is accessible and properly configured

#### 🔍 Debugging Steps

1. **Validate Setup**: Always run `bash validate-setup.sh` after setup
2. **Check Logs**: Review container logs for specific error messages
3. **Test Connectivity**: Use the architecture showcase notebook to test all components
4. **Resource Monitoring**: Monitor Docker resource usage during operations

### Getting Help

- **Validation Script**: Run `bash validate-setup.sh` for comprehensive diagnostics
- **Architecture Showcase**: Use `work/projects/architecture_showcase.ipynb` to test functionality
- **Service URLs**: Access individual service UIs for detailed monitoring
  - Spark Master: http://localhost:5050
  - MinIO Console: http://localhost:9090
  - Trino: http://localhost:8080