import argparse
import sys
import os
from datetime import datetime, timedelta

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import Row
from src.common.spark_utils import get_spark_session
from src.features.user_features import UserFeatures
from src.validation.validator import Validator

def generate_mock_data(spark, start_date, days=1):
    data = []
    base_time = datetime.strptime(start_date, "%Y-%m-%d")
    for i in range(days):
        day = base_time + timedelta(days=i)
        # User 1 active
        data.append(Row(user_id="u1", timestamp=day.replace(hour=10), event_type="click", value=1.0))
        data.append(Row(user_id="u1", timestamp=day.replace(hour=11), event_type="purchase", value=100.0))
        # User 2 active
        data.append(Row(user_id="u2", timestamp=day.replace(hour=12), event_type="click", value=0.0))
    
    return spark.createDataFrame(data)

def run_backfill(start_date, end_date):
    spark = get_spark_session()
    
    # 1. Source Data (Mock)
    print(f"Generating mock data from {start_date}...")
    raw_df = generate_mock_data(spark, start_date, days=2) # Generate 2 days for demo
    
    # 2. Compute Features
    feature_group = UserFeatures()
    print(f"Computing features for {feature_group.name}...")
    features_df = feature_group.compute(raw_df)
    features_df.show()
    
    # 3. Validation
    print("Validating features...")
    if not Validator.validate_quality(features_df, {"user_id": "not_null", "total_value": "not_null"}):
        raise ValueError("Data Validation Failed!")
        
    # 4. Write to Iceberg
    table_name = f"local.default.{feature_group.name}"
    
    # Check if table exists, create if not
    # In production, use DDL scripts. Here we use DataFrameWriterV2 if available or simple write
    print(f"Writing to Iceberg table: {table_name}")
    
    # Using 'overwrite' for backfill idempotency on the partition level usually, 
    # but for simplicity using append or create.
    # To handle 'backfill' properly with Iceberg, we usually use overwrite_partitions() dynamic or merge.
    # Here we default to 'append' for first run, or 'overwrite' for simple full replacement demo.
    
    features_df.writeTo(table_name) \
        .createOrReplace() 
    
    print("Write committed.")
    
    # 5. Versioning / Time Travel info
    print("\n--- Table History (Snapshots) ---")
    spark.sql(f"SELECT * FROM {table_name}.history").show(truncate=False)
    
    print("\n--- Data Verification ---")
    spark.sql(f"SELECT * FROM {table_name}").show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", default="2023-01-01")
    parser.add_argument("--end_date", default="2023-01-02")
    args = parser.parse_args()
    
    run_backfill(args.start_date, args.end_date)
