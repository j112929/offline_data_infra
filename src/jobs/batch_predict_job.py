"""
Batch Prediction Job - Scalable offline batch inference using Spark.

This module provides:
- High-throughput batch inference using Spark UDFs
- Integration with Model Registry for model versioning
- Automatic feature joining from Iceberg tables
- Result storage back to Iceberg with full lineage

Example:
    >>> job = BatchPredictionJob(
    ...     model_name="user_ctr_model",
    ...     input_table="local.default.user_features_daily",
    ...     output_table="local.default.user_predictions"
    ... )
    >>> job.run(prediction_date="2023-01-01")
"""

import argparse
import sys
import os
import uuid
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any, Callable
from dataclasses import dataclass

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, lit, struct, current_timestamp
from pyspark.sql.types import DoubleType, ArrayType, StructType, StructField, StringType

from src.common.spark_utils import get_spark_session
from src.common.git_utils import get_git_commit_hash
from src.model.registry import ModelRegistry, ModelVersion

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class BatchPredictionConfig:
    """Configuration for batch prediction job."""
    model_name: str
    model_version: Optional[str] = None  # None = use production version
    input_table: str = "local.default.user_features_daily"
    output_table: str = "local.default.batch_predictions"
    feature_columns: Optional[List[str]] = None  # None = auto-detect
    entity_column: str = "user_id"
    prediction_column: str = "prediction"
    output_mode: str = "append"  # append, overwrite
    partition_by: Optional[List[str]] = None
    batch_size: int = 10000


class BatchPredictionJob:
    """
    Spark-based batch prediction job with Model Registry integration.
    
    Features:
    - Loads model from Model Registry
    - Joins features from Iceberg tables
    - Produces predictions with full lineage
    - Writes results back to Iceberg
    """
    
    def __init__(
        self,
        config: BatchPredictionConfig,
        registry_path: str = "./model_registry"
    ):
        """
        Initialize the batch prediction job.
        
        Args:
            config: BatchPredictionConfig object
            registry_path: Path to the model registry
        """
        self.config = config
        self.registry = ModelRegistry(registry_path)
        self.spark: Optional[SparkSession] = None
        self.model = None
        self.model_version: Optional[ModelVersion] = None
        self.run_id = str(uuid.uuid4())[:8]
        
    def _load_model(self) -> Any:
        """Load model from registry."""
        if self.config.model_version:
            self.model_version = self.registry.get_model_version(
                self.config.model_name,
                self.config.model_version
            )
        else:
            # Use production model
            self.model_version = self.registry.get_production_model(self.config.model_name)
        
        if not self.model_version:
            raise ValueError(
                f"Model {self.config.model_name}:{self.config.model_version or 'production'} not found"
            )
        
        logger.info(f"Loading model: {self.model_version.model_name}:{self.model_version.version}")
        logger.info(f"  └── Stage: {self.model_version.stage.value}")
        logger.info(f"  └── Metrics: {self.model_version.metrics}")
        
        # Load model based on format
        model_path = self.model_version.model_path
        model_format = self.model_version.model_format
        
        if model_format == "pickle":
            import pickle
            with open(model_path, 'rb') as f:
                self.model = pickle.load(f)
        elif model_format == "onnx":
            import onnxruntime as ort
            self.model = ort.InferenceSession(model_path)
        elif model_format == "torchscript":
            import torch
            self.model = torch.jit.load(model_path)
        else:
            raise ValueError(f"Unsupported model format: {model_format}")
        
        return self.model
    
    def _create_prediction_udf(self) -> Callable:
        """
        Create a Spark UDF for prediction.
        
        Note: In production, you would broadcast the model to executors
        or use pandas_udf for better performance.
        """
        model = self.model
        model_format = self.model_version.model_format
        
        def predict_fn(features):
            """Inner prediction function."""
            if model is None:
                return 0.0
            
            # Convert features to appropriate format
            if hasattr(model, 'predict'):
                # sklearn-like
                import numpy as np
                feature_array = np.array([features]).reshape(1, -1)
                return float(model.predict(feature_array)[0])
            elif hasattr(model, 'predict_proba'):
                import numpy as np
                feature_array = np.array([features]).reshape(1, -1)
                return float(model.predict_proba(feature_array)[0][1])
            else:
                # Mock prediction for demo
                return sum(features) / len(features) if features else 0.0
        
        return udf(predict_fn, DoubleType())
    
    def _read_features(self, prediction_date: Optional[str] = None) -> DataFrame:
        """Read features from Iceberg table."""
        df = self.spark.read.format("iceberg").load(self.config.input_table)
        
        # Filter by date if specified
        if prediction_date and "date" in df.columns:
            df = df.filter(col("date") == prediction_date)
        
        logger.info(f"Loaded {df.count()} rows from {self.config.input_table}")
        return df
    
    def _prepare_features(self, df: DataFrame) -> DataFrame:
        """Prepare features for prediction."""
        # Determine feature columns
        if self.config.feature_columns:
            feature_cols = self.config.feature_columns
        else:
            # Auto-detect: exclude entity column and date
            exclude_cols = {self.config.entity_column, "date", "timestamp", "created_at"}
            feature_cols = [c for c in df.columns if c not in exclude_cols]
        
        logger.info(f"Using feature columns: {feature_cols}")
        
        # For simplicity, collect features into array
        # In production, use VectorAssembler or pandas_udf
        from pyspark.sql.functions import array
        df = df.withColumn("_features", array(*[col(c).cast("double") for c in feature_cols]))
        
        return df, feature_cols
    
    def _add_lineage_columns(self, df: DataFrame) -> DataFrame:
        """Add lineage columns for traceability."""
        return df.withColumn("_prediction_run_id", lit(self.run_id)) \
                 .withColumn("_model_name", lit(self.model_version.model_name)) \
                 .withColumn("_model_version", lit(self.model_version.version)) \
                 .withColumn("_feature_snapshot", lit(self.model_version.metadata.feature_snapshot_id)) \
                 .withColumn("_predicted_at", current_timestamp())
    
    def _write_predictions(self, df: DataFrame):
        """Write predictions to Iceberg table."""
        # Ensure table exists
        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.config.output_table} (
                    {self.config.entity_column} STRING,
                    date DATE,
                    {self.config.prediction_column} DOUBLE,
                    _prediction_run_id STRING,
                    _model_name STRING,
                    _model_version STRING,
                    _feature_snapshot STRING,
                    _predicted_at TIMESTAMP
                ) USING iceberg
            """)
        except Exception as e:
            logger.warning(f"Table creation note: {e}")
        
        # Select relevant columns
        output_cols = [
            self.config.entity_column,
            "date",
            self.config.prediction_column,
            "_prediction_run_id",
            "_model_name",
            "_model_version",
            "_feature_snapshot",
            "_predicted_at"
        ]
        
        # Filter to only existing columns
        existing_cols = set(df.columns)
        output_cols = [c for c in output_cols if c in existing_cols]
        
        output_df = df.select(*output_cols)
        
        # Write based on mode
        if self.config.output_mode == "overwrite":
            output_df.writeTo(self.config.output_table).overwrite()
        else:
            output_df.writeTo(self.config.output_table).append()
        
        logger.info(f"Written predictions to {self.config.output_table}")
    
    def run(self, prediction_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute the batch prediction job.
        
        Args:
            prediction_date: Optional date filter (format: YYYY-MM-DD)
        
        Returns:
            Job summary with stats
        """
        start_time = datetime.now()
        logger.info(f"=" * 60)
        logger.info(f"Starting Batch Prediction Job")
        logger.info(f"Run ID: {self.run_id}")
        logger.info(f"Model: {self.config.model_name}")
        logger.info(f"=" * 60)
        
        # Initialize Spark
        self.spark = get_spark_session(app_name=f"BatchPrediction_{self.run_id}")
        
        # Set lineage metadata
        commit_hash = get_git_commit_hash()
        self.spark.conf.set("spark.snapshot-property.prediction-run-id", self.run_id)
        self.spark.conf.set("spark.snapshot-property.code-version", commit_hash)
        
        # Load model
        self._load_model()
        
        # Read features
        features_df = self._read_features(prediction_date)
        
        if features_df.count() == 0:
            logger.warning("No data to predict. Exiting.")
            return {"status": "empty", "rows_processed": 0}
        
        # Prepare features
        prepared_df, feature_cols = self._prepare_features(features_df)
        
        # Create prediction UDF
        predict_udf = self._create_prediction_udf()
        
        # Apply predictions
        logger.info("Applying model predictions...")
        predictions_df = prepared_df.withColumn(
            self.config.prediction_column,
            predict_udf(col("_features"))
        )
        
        # Add lineage
        predictions_df = self._add_lineage_columns(predictions_df)
        
        # Cache for multiple operations
        predictions_df = predictions_df.cache()
        row_count = predictions_df.count()
        
        # Compute summary stats
        stats = predictions_df.select(
            self.config.prediction_column
        ).summary("count", "mean", "stddev", "min", "max").collect()
        
        stats_dict = {row["summary"]: row[self.config.prediction_column] for row in stats}
        
        # Write results
        self._write_predictions(predictions_df)
        
        # Cleanup
        predictions_df.unpersist()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        job_summary = {
            "status": "success",
            "run_id": self.run_id,
            "model": f"{self.model_version.model_name}:{self.model_version.version}",
            "feature_snapshot_id": self.model_version.metadata.feature_snapshot_id,
            "input_table": self.config.input_table,
            "output_table": self.config.output_table,
            "prediction_date": prediction_date,
            "rows_processed": row_count,
            "prediction_stats": stats_dict,
            "duration_seconds": duration,
            "git_commit": commit_hash
        }
        
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Batch Prediction Complete!")
        logger.info(f"  └── Rows: {row_count}")
        logger.info(f"  └── Duration: {duration:.2f}s")
        logger.info(f"  └── Prediction Mean: {stats_dict.get('mean', 'N/A')}")
        logger.info(f"{'=' * 60}")
        
        return job_summary


class MockBatchPredictionJob:
    """
    Mock Batch Prediction Job for demonstration without requiring a real model.
    
    This simulates batch predictions using simple heuristics.
    """
    
    def __init__(
        self,
        input_table: str = "local.default.user_features_daily",
        output_table: str = "local.default.batch_predictions",
        model_name: str = "mock_ctr_model",
        model_version: str = "1.0.0"
    ):
        self.input_table = input_table
        self.output_table = output_table
        self.model_name = model_name
        self.model_version = model_version
        self.run_id = str(uuid.uuid4())[:8]
    
    def run(self, prediction_date: Optional[str] = None) -> Dict[str, Any]:
        """Execute mock batch prediction."""
        spark = get_spark_session(app_name=f"MockBatchPrediction_{self.run_id}")
        
        logger.info(f"Starting Mock Batch Prediction - Run ID: {self.run_id}")
        
        # Read features
        try:
            df = spark.read.format("iceberg").load(self.input_table)
        except Exception as e:
            logger.error(f"Failed to read from {self.input_table}: {e}")
            return {"status": "error", "error": str(e)}
        
        if df.count() == 0:
            logger.warning("No data found")
            return {"status": "empty"}
        
        # Mock prediction: use total_value as a simple predictor
        from pyspark.sql.functions import when, rand
        
        predictions_df = df.withColumn(
            "prediction",
            # Mock CTR prediction based on features
            when(col("total_value") > 50, 0.7 + rand() * 0.3)
            .when(col("total_value") > 10, 0.3 + rand() * 0.4)
            .otherwise(0.1 + rand() * 0.2)
        ).withColumn("_prediction_run_id", lit(self.run_id)) \
         .withColumn("_model_name", lit(self.model_name)) \
         .withColumn("_model_version", lit(self.model_version)) \
         .withColumn("_predicted_at", current_timestamp())
        
        # Write to output table
        try:
            # Create table if not exists
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.output_table} (
                    user_id STRING,
                    date DATE,
                    event_count LONG,
                    total_value DOUBLE,
                    prediction DOUBLE,
                    _prediction_run_id STRING,
                    _model_name STRING,
                    _model_version STRING,
                    _predicted_at TIMESTAMP
                ) USING iceberg
            """)
            
            predictions_df.writeTo(self.output_table).append()
            row_count = predictions_df.count()
            
            logger.info(f"✅ Written {row_count} predictions to {self.output_table}")
            
            # Show sample
            logger.info("\nSample Predictions:")
            predictions_df.select("user_id", "total_value", "prediction", "_model_version").show()
            
            return {
                "status": "success",
                "run_id": self.run_id,
                "rows_processed": row_count,
                "output_table": self.output_table
            }
            
        except Exception as e:
            logger.error(f"Failed to write predictions: {e}")
            return {"status": "error", "error": str(e)}


def run_batch_prediction(
    model_name: str,
    input_table: str,
    output_table: str,
    model_version: Optional[str] = None,
    prediction_date: Optional[str] = None,
    use_mock: bool = False
) -> Dict[str, Any]:
    """
    Convenience function to run batch prediction.
    
    Args:
        model_name: Name of the model in registry
        input_table: Iceberg table with features
        output_table: Iceberg table for predictions
        model_version: Specific version (None = production)
        prediction_date: Date filter
        use_mock: Use mock prediction for demo
    
    Returns:
        Job summary dict
    """
    if use_mock:
        job = MockBatchPredictionJob(
            input_table=input_table,
            output_table=output_table,
            model_name=model_name,
            model_version=model_version or "1.0.0"
        )
    else:
        config = BatchPredictionConfig(
            model_name=model_name,
            model_version=model_version,
            input_table=input_table,
            output_table=output_table
        )
        job = BatchPredictionJob(config)
    
    return job.run(prediction_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch Prediction Job")
    parser.add_argument("--model_name", default="user_ctr_model", help="Model name in registry")
    parser.add_argument("--model_version", default=None, help="Model version (None = production)")
    parser.add_argument("--input_table", default="local.default.user_features_daily")
    parser.add_argument("--output_table", default="local.default.batch_predictions")
    parser.add_argument("--prediction_date", default=None)
    parser.add_argument("--mock", action="store_true", help="Use mock prediction")
    
    args = parser.parse_args()
    
    result = run_batch_prediction(
        model_name=args.model_name,
        input_table=args.input_table,
        output_table=args.output_table,
        model_version=args.model_version,
        prediction_date=args.prediction_date,
        use_mock=args.mock
    )
    
    print(f"\nJob Result: {result}")
