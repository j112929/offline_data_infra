import argparse
import sys
import os
import logging
from datetime import datetime, timedelta

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.common.spark_utils import get_spark_session

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def run_maintenance(table_name, older_than_days=7, compact_files=True):
    """
    Runs maintenance tasks on an Iceberg table:
    1. Expire Snapshots (clean up old history/data files).
    2. Remove Orphan Files (clean up unreferenced files).
    3. Rewrite Data Files (Compaction - merge small files).
    """
    spark = get_spark_session(app_name="IcebergMaintenance")
    
    # Calculate timestamp for expiration
    expiry_ts = datetime.now() - timedelta(days=older_than_days)
    logger.info(f"--- Starting Maintenance for {table_name} ---")
    logger.info(f"Expiring snapshots older than: {expiry_ts}")

    # 1. Expire Snapshots
    # This removes the metadata entries for old snapshots AND deletes the underlying data files if no longer needed.
    try:
        # Note: In real Spark SQL Iceberg extensions, we use CALL system.expire_snapshots
        # Syntax: CALL catalog.system.expire_snapshots(table, older_than_timestamp, retain_last)
        
        # We'll use a safer approach keeping at least 1 snapshot if older_than is aggressive
        query = f"CALL local.system.expire_snapshots('{table_name}', TIMESTAMP '{expiry_ts}', 1)"
        logger.info(f"Executing: {query}")
        spark.sql(query).show()
    except Exception as e:
        logger.error(f"Expire Snapshots Failed: {e}")

    # 2. Remove Orphan Files
    # Files that might be left over from failed jobs or Spark driver crashes.
    try:
        query = f"CALL local.system.remove_orphan_files('{table_name}', TIMESTAMP '{expiry_ts}')"
        logger.info(f"Executing: {query}")
        spark.sql(query).show()
    except Exception as e:
        logger.error(f"Remove Orphan Files Failed: {e}")

    # 3. Compaction (Rewrite Data Files)
    if compact_files:
        logger.info("Compacting small files...")
        try:
            # Syntax: CALL catalog.system.rewrite_data_files(table)
            # You can add options like 'target-file-size-bytes' etc.
            query = f"CALL local.system.rewrite_data_files(table => '{table_name}')"
            logger.info(f"Executing: {query}")
            spark.sql(query).show()
        except Exception as e:
            logger.error(f"Compaction Failed: {e}")

    logger.info("Maintenance Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_name", default="local.default.user_features_daily")
    parser.add_argument("--days", type=int, default=7, help="Expire snapshots older than N days")
    parser.add_argument("--compact", type=bool, default=True, help="Run compaction")
    args = parser.parse_args()
    
    run_maintenance(args.table_name, args.days, args.compact)
