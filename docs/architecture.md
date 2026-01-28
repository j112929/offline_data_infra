# Architecture Design

## 1. Lakehouse Storage (Iceberg)

We use Apache Iceberg as the unifying storage layer.

### Why Iceberg?
*   **Snapshots**: Every commit creates a snapshot.
*   **Partitioning**: Hidden partitioning (e.g., by Day) allows efficient access for backfills.
*   **Branching & Tagging**: Enables sophisticated lifecycles like WAP (Write-Audit-Publish).

## 2. Feature Backfill Strategy (WAP Implemented)

We have implemented the **Write-Audit-Publish (WAP)** pattern to ensure zero dirty data in production.

### Workflow
1.  **Branch Creation**: `audit_<run_id>` branch created.
2.  **Write Isolation**: Data written to branch only.
3.  **Automated Audit**: Validation runs on branch.
4.  **Publish**: `fast_forward` main to branch if valid.

### Versioning & Lineage
*   Metadata: Code Git Hash + Run ID injected into every Snapshot.
*   Time Travel SQL: `SELECT * FROM table FOR SYSTEM_VERSION AS OF ...`

## 3. Serving Layer (Batch-Stream Consistency)
*   **Incremental Sync**: `OnlineSyncJob` reads Iceberg delta (`start-snapshot-id`) and upserts to Redis.
*   **Consistency**: Unified source of truth is the Iceberg table validated snapshots.

## 4. Maintenance & Operations
To prevent performance degradation over time:
*   **Compaction**: `rewrite_data_files` merges small files (created by frequent streaming or incremental writes) into larger, read-optimized files.
*   **Expiration**: `expire_snapshots` removes history older than N days (default 7) to reclaim storage and keep metadata manageable.
*   **Orphan Cleanup**: `remove_orphan_files` deletes data files no longer referenced by any valid metadata.

## 5. Directory Structure
*   `src/jobs/backfill_job.py`: Main WAP engine.
*   `src/jobs/online_sync_job.py`: Pushes to Serving Store.
*   `src/jobs/maintenance_job.py`: Optimization & Cleanup.
