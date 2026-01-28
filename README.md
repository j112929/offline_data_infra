# Offline Feature Backfill Platform (Lakehouse Architecture)

This project implements an Offline Feature Backfill Platform using **Apache Iceberg** and **Apache Spark**.

## Core Objectives

1.  **History Reproducibility (Time Travel)**: Leverage Iceberg's snapshot mechanism to query data `AS OF` significant points in time, ensuring models can be retrained on exact historical states.
2.  **Data Version Governance**: Manage feature processing code versions alongside data versions (Iceberg snapshots).
3.  **Batch-Stream Consistency**: Ensure that offline backfills (Batch) and online serving (Stream) use unified feature definitions (Feature Store methodology).

## Architecture

```mermaid
graph LR
    Raw[Raw Events] -->|Spark Batch| IcebergTable[Iceberg Feature Table]
    IcebergTable -->|Validation| QualityCheck[Data Quality Check]
    QualityCheck -->|Pass| Training[Model Training]
    
    subgraph "Lakehouse (Iceberg)"
        IcebergTable
        Versions[Snapshots / History]
    end
    
    subgraph "Backfill Engine"
        Job[Spark Backfill Job]
        Config[Backfill Config (TimeRange)]
    end
    
    Config --> Job
    Job --> IcebergTable
```

## implementation Details

### 1. Technology Stack
*   **Storage**: Apache Iceberg (Supports ACID, Time Travel, Schema Evolution).
*   **Compute**: Apache Spark (PySpark).
*   **Validation**: PySpark DataFrame checks (extensible to Great Expectations).

### 2. Key Flows

#### Backfill Flow
1.  **Configuration**: User defines the feature logic and the target time range (e.g., `2023-01-01` to `2023-12-31`).
2.  **Execution**: Spark job computes features from Raw Data.
3.  **Write Strategy**: Uses `WAP` (Write-Audit-Publish) pattern or `merge_into` / `overwrite_partitions` to safely update the Iceberg table without affecting concurrent readers.
4.  **Versioning**: A new Iceberg snapshot is committed.

#### Validation Flow
BEFORE exposing new data for training:
1.  Check for `null` rates.
2.  Check for distribution shifts (basic statistical summary).
3.  Ensure primary key uniqueness.

## Directory Structure

```
offline_data_infra/
├── docs/               # Architecture diagrams and detailed docs
├── src/
│   ├── jobs/           # Spark jobs for backfill
│   ├── features/       # Feature extraction logic
│   └── validation/     # Data quality checks
├── tests/              # Unit and Integration tests
└── requirements.txt    # Python dependencies
```

## Getting Started

*(Instructions to set up local user environment for Spark + Iceberg)*
