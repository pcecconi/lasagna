from IPython.core.magic import register_line_magic
from IPython import get_ipython
from IPython.display import display, Markdown

@register_line_magic
def SparkSession(line):
    
    hive_script = """from pyspark.sql import SparkSession

spark = SparkSession\\
    .builder\\
    .appName("SparkSession-Hive")\\
    .config("spark.driver.memory", "2g")\\
    .config("spark.driver.maxResultSize", "1g")\\
    .config("spark.executor.memory", "512m")\\
    .config("spark.executor.cores", "2")\\
    .config("spark.dynamicAllocation.enabled", "true")\\
    .config("spark.dynamicAllocation.maxExecutors", "4")\\
    .config("spark.sql.adaptive.enabled", "true")\\
    .getOrCreate()

spark"""

    delta_script = """from pyspark.sql import SparkSession

spark = SparkSession \\
    .builder \\
    .appName("SparkSession-Delta") \\
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \\
    .config("spark.driver.memory", "2g") \\
    .config("spark.driver.maxResultSize", "1g") \\
    .config("spark.executor.memory", "512m") \\
    .config("spark.executor.cores", "2") \\
    .config("spark.dynamicAllocation.enabled", "true") \\
    .config("spark.dynamicAllocation.maxExecutors", "4") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .enableHiveSupport() \\
    .getOrCreate()

spark"""

    iceberg_script = """from pyspark.sql import SparkSession

spark = SparkSession \\
    .builder \\
    .appName("SparkSession-Iceberg") \\
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0") \\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.iceberg.type", "hive") \\
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \\
    .config("spark.driver.memory", "2g") \\
    .config("spark.driver.maxResultSize", "1g") \\
    .config("spark.executor.memory", "512m") \\
    .config("spark.executor.cores", "2") \\
    .config("spark.dynamicAllocation.enabled", "true") \\
    .config("spark.dynamicAllocation.maxExecutors", "4") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .getOrCreate()

spark""" 
    
    shell = get_ipython()
    profile = shell.kernel.config['IPKernelApp']['profile']
    profile_name = str(profile).replace('pyspark_', '').capitalize()
    
    if profile == 'pyspark_delta':
        script = delta_script
    elif profile == 'pyspark_iceberg':
        script = iceberg_script
    else:
        script = hive_script
        profile_name = 'Hive'
    
    
        
    session_details = f"""# SparkSession - {profile_name}
| Spark Session        | http://localhost:4040        |
|----------------------|------------------------------|
| Spark Master         | http://localhost:5050        |
| Spark Worker A       | http://localhost:5051        |
| Spark Worker B       | http://localhost:5052        | 
| Hive Metastore       | thrift://hive-metastore:9083 |

| MinIO Object Storage | http://localhost:9090 |
|----------------------|-----------------------|
| user                 | `admin`               |
| password             | `password`            |

- Use `%trino` or `%%trino` to run queries using Trino
- Use `%sparksql` or `%%sparksql` to run queries using SparkSQL
"""

    shell.set_next_input(f"{script}", replace=False)
    
    return display(Markdown(session_details))