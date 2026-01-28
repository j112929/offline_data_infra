from pyspark.sql import SparkSession
import os

def get_spark_session(app_name="OfflineFeaturePlatform", warehouse_dir="./warehouse"):
    """
    Creates a SparkSession configured for Apache Iceberg (Local Hadoop Catalog).
    """
    # Ensure warehouse directory exists
    os.makedirs(warehouse_dir, exist_ok=True)
    abs_warehouse_path = os.path.abspath(warehouse_dir)

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", abs_warehouse_path) \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1") 
    
    # Note: spark.jars.packages will download the jar on first run. 
    # Ensure Java is installed and compatible (Java 8/11/17).
    
    return builder.getOrCreate()
