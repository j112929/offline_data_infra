import argparse
import sys
import os
import json

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.common.spark_utils import get_spark_session
from src.serving.online_store import MockRedisStore

def get_last_synced_snapshot(state_file=".sync_state"):
    """
    Reads the last synced snapshot ID from a local state file.
    In production, this state would be stored in ZooKeeper, DynamoDB, or the Online Store itself.
    """
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return json.load(f).get("last_snapshot_id")
    return None

def update_sync_state(snapshot_id, state_file=".sync_state"):
    with open(state_file, "w") as f:
        json.dump({"last_snapshot_id": snapshot_id}, f)

def run_online_sync(table_name):
    spark = get_spark_session()
    online_store = MockRedisStore()
    
    # 1. Determine read range (Incremental or Full)
    last_snapshot = get_last_synced_snapshot()
    
    print(f"--- Starting Sync for {table_name} ---")
    
    df_reader = spark.read.format("iceberg")
    
    if last_snapshot:
        print(f"Incremental Sync initiating from snapshot: {last_snapshot}")
        # Incremental read: start-snapshot-id (exclusive) to current
        # Note: If last_snapshot is too old (gc-ed), this might fail. 
        # Production systems need to handle 'SnapshotExpiredException' and fallback to full sync.
        try:
            # We assume we want to read FROM the snapshot AFTER the last synced one
            # Iceberg option 'start-snapshot-id' excludes the ID itself usually in changelog,
            # or we can use 'applies-to' logic. 
            # For pure append reading:
            changes_df = df_reader.option("start-snapshot-id", last_snapshot).load(table_name)
        except Exception as e:
            print(f"Warning: Could not perform incremental read ({e}). Falling back to full load (careful with volume!).")
            changes_df = df_reader.load(table_name)
    else:
        print("No state found. Performing Initial Full Sync.")
        changes_df = df_reader.load(table_name)

    # 2. Get current Snapshot ID (to update state later)
    # We grab the snapshot ID of the loaded dataframe relation
    # In Iceberg Spark, getting the exact snapshot ID of the read operation is tricky 
    # if not explicitly querying metadata.
    # Safe approach: Query the table history and pick the latest one processed.
    
    latest_history = spark.sql(f"SELECT snapshot_id FROM {table_name}.history ORDER BY committed_at DESC LIMIT 1").collect()
    if not latest_history:
        print("Table is empty. Nothing to sync.")
        return

    current_snapshot_id = latest_history[0]['snapshot_id']
    
    if last_snapshot == current_snapshot_id:
        print("Already up to date.")
        return

    # 3. Process and Write
    # Ideally use foreachPartition for high throughput
    print(f"Syncing data to Online Store...")
    
    # Simple collect for demo (DO NOT use collect in production for large data!)
    # Production: df.rdd.mapPartitions(lambda iter: write_batch(iter)).count()
    records = [row.asDict() for row in changes_df.collect()]
    
    # Convert dates/decimals to serializable format if needed
    for r in records:
        if 'date' in r: r['date'] = str(r['date'])
    
    online_store.batch_write(records)
    
    # 4. Commit State
    update_sync_state(current_snapshot_id)
    print(f"Sync Complete. Updated state to snapshot: {current_snapshot_id}")
    
    # Verification (Mock)
    print("\n--- Online Store State (Mock) ---")
    print(json.dumps(online_store.data, indent=2, default=str))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_name", default="local.default.user_features_daily")
    args = parser.parse_args()
    
    run_online_sync(args.table_name)
