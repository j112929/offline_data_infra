# Architecture Design

## 1. Lakehouse Storage (Iceberg)

We use Apache Iceberg as the unifying storage layer.

### Why Iceberg?
*   **Snapshots**: Every commit creates a snapshot. We can tag snapshots with Git commits of the feature generation code for full lineage.
*   **Partitioning**: Hidden partitioning (e.g., by Day) allows efficient access for backfills.
*   **Concurrent Writers**: Optimistic concurrency control allows backfills to run while streaming jobs might be appending (if designed carefully).

## 2. Feature Backfill Strategy

### Idempotency
Backfills are operationally idempotent.
*   **Granularity**: Partition-level (e.g., `date`).
*   **Mode**: `overwrite_partitions (dynamic)`.

When a backfill job runs for `2023-01-01` to `2023-01-05`:
1.  Spark computes the dataframe.
2.  Iceberg detects involved partitions.
3.  Atomic swap of the new data for those partitions.

### Versioning & Time Travel
To reproduce a model training set from 3 months ago:
```sql
SELECT * FROM features 
FOR SYSTEM_VERSION AS OF '2023-06-01 12:00:00'
WHERE date BETWEEN '2023-01-01' AND '2023-06-01'
```
This guarantees that even if we fixed a bug in the feature logic *today*, we can still retrieve the *exact* data used for the old model version.

## 3. Batch-Stream Consistency

To ensure the Online Store (Redis) matches the Offline Store (Iceberg):
1.  **Single Logic Source**: The `FeatureGroup.compute` method (in `src/features`) is the source of truth.
2.  **Streaming**:
    *   Ideally, use Flink/Spark Streaming to run the *same* logic.
    *   Sink to Online Store (Hot) AND Iceberg (Cold).
3.  **Lambda Architecture (Simplified)**:
    *   Stream writes to Online Store + Iceberg (Raw Events).
    *   Periodic Batch (Hourly/Daily) compacts and corrects Iceberg data.
    *   **consistency check**: Compare Online Store values against Iceberg table for a sample set of keys.

## 4. Validation

We implement "Write-Audit-Publish" (WAP) validation.
1.  **Write**: Backfill job writes data to a *staging branch* or a non-live snapshot.
2.  **Audit**: Validation tool queries the staging snapshot. checks distribution.
3.  **Publish**: If valid, fast-forward the `main` branch to the staging snapshot. 
    *   *Note: In the simplified code, we validate DataFrame before write, which is simpler but less robust against write-failures.*
