import argparse
import sys
import os
import uuid
import logging
from datetime import datetime, timedelta

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import Row
from src.common.spark_utils import get_spark_session
from src.common.git_utils import get_git_commit_hash
from src.features.user_features import UserFeatures
from src.validation.validator import Validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def generate_mock_data(spark, start_date, days=1):
    data = []
    base_time = datetime.strptime(start_date, "%Y-%m-%d")
    for i in range(days):
        day = base_time + timedelta(days=i)
        data.append(Row(user_id="u1", timestamp=day.replace(hour=10), event_type="click", value=1.0))
        data.append(Row(user_id="u1", timestamp=day.replace(hour=11), event_type="purchase", value=100.0))
        data.append(Row(user_id="u2", timestamp=day.replace(hour=12), event_type="click", value=0.0))
    return spark.createDataFrame(data)

def run_backfill(start_date, end_date):
    spark = get_spark_session()
    
    # --- 1. Context & Lineage Setup ---
    commit_hash = get_git_commit_hash()
    run_id = str(uuid.uuid4())[:8]
    logger.info(f"Starting Backfill. RunID: {run_id}, Commit: {commit_hash}")

    # Inject metadata into Iceberg Snapshots
    spark.conf.set("spark.snapshot-property.code-version", commit_hash)
    spark.conf.set("spark.snapshot-property.run-id", run_id)
    spark.conf.set("spark.snapshot-property.pipeline-user", "jizhuolin")

    # --- 2. Compute Features ---
    logger.info("Generating Mock Data...")
    raw_df = generate_mock_data(spark, start_date, days=2)
    
    feature_group = UserFeatures()
    logger.info(f"Computing features for {feature_group.name}...")
    features_df = feature_group.compute(raw_df)

    # --- 3. WAP: Write to Audit Branch ---
    table_name = f"local.default.{feature_group.name}"
    audit_branch = f"audit_{run_id}"
    
    # Ensure table exists (create empty if not)
    # Using simple CreateIfNotExists logic
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} (user_id string, date date, event_count long, total_value double) USING iceberg")
    except Exception as e:
        logger.warning(f"Table creation check skipped or failed: {e}")

    # Create Branch
    logger.info(f"Creating Audit Branch: {audit_branch}")
    try:
        # If branch execution fails (e.g. table doesn't exist yet generally), we handle logic
        spark.sql(f"ALTER TABLE {table_name} CREATE BRANCH IF NOT EXISTS {audit_branch}")
    except Exception as e:
        # If table was just created, it might be empty. 
        # Writing to branch on empty table works if created.
        logger.info(f"Branch creation note: {e}")

    # Write to Branch
    logger.info(f"Writing data to branch: {audit_branch}")
    # Spark 3.4+ Iceberg supports "table.branch_xyz" syntax
    # Or use spark.wap.branch conf
    
    # We use explicit branch write syntax
    write_target = f"{table_name}.branch_{audit_branch}"
    
    # Use append for backfill logic in this demo (or overwrite_partitions)
    features_df.writeTo(write_target).append()

    # --- 4. Validation (Audit) ---
    logger.info(f"Validating data in branch: {audit_branch}")
    audit_df = spark.read.format("iceberg").option("branch", audit_branch).load(table_name)
    
    # Run Quality Checks
    is_valid = Validator.validate_quality(audit_df, {"user_id": "not_null", "total_value": "not_null"})
    
    if is_valid:
        logger.info("Validation PASSED. Fast-forwarding main to audit branch...")
        
        # --- 5. Publish (Fast Forward) ---
        # Note: cherrypick_snapshot or fast_forward procedure
        # 'cherrypick_snapshot' is robust for single snapshot commits. 
        # 'fast_forward' updates the ref to point to branch head.
        
        # We need the snapshot ID from the branch head to cherrypick, OR just use fast_forward procedure
        try:
            # Using Iceberg Procedure (requires 'call')
            # Assuming 'local' supports procedures. If not, we fall back to cherrypick.
            
            # fast_forward(table, branch, to) -> updates 'to' (main) to match 'branch'
            # Note: The argument order depends on Iceberg version. 
            # Often: CALL catalog.system.fast_forward(table, branch, to_branch)
            # Default to_branch is 'main'.
            
            spark.sql(f"CALL local.system.fast_forward('{table_name}', '{audit_branch}', 'main')")
            logger.info("Successfully Published to Main!")
            
        except Exception as e:
            logger.warning(f"Fast Forward failed: {e}. Attempting fallback via cherrypick_snapshot...")
            try:
                # Fallback: Get snapshot ID of the audit branch
                # Spark 3.4+ Iceberg table.refs
                branch_info = spark.sql(f"SELECT snapshot_id FROM {table_name}.refs WHERE name = '{audit_branch}'").collect()
                if branch_info:
                    snap_id = branch_info[0]['snapshot_id']
                    logger.info(f"Cherry-picking snapshot {snap_id} from {audit_branch}...")
                    spark.sql(f"CALL local.system.cherrypick_snapshot('{table_name}', {snap_id})")
                    logger.info("Successfully Published via Cherry-Pick!")
                else:
                    raise ValueError(f"Could not find snapshot for branch {audit_branch}")
            except Exception as e2:
                logger.error(f"Publish failed completely: {e2}")
                raise e2
            
    else:
        logger.error("Validation FAILED. Data remains in audit branch for debugging. Main branch is untouched.")
        raise ValueError("Data Quality Validation Failed.")

    logger.info("History including Code Lineage:")
    spark.sql(f"SELECT committed_at, snapshot_id, summary['spark.snapshot-property.code-version'] as git_hash, summary['spark.snapshot-property.run-id'] as run_id FROM {table_name}.snapshots ORDER BY committed_at DESC").show(truncate=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", default="2023-01-01")
    parser.add_argument("--end_date", default="2023-01-02")
    args = parser.parse_args()
    
    run_backfill(args.start_date, args.end_date)
