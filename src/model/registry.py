"""
Model Registry - Central repository for ML model versioning and lineage tracking.

This module provides:
- Model versioning with semantic versioning support
- Feature snapshot binding (code-data lineage)
- Model lifecycle management (staging, production, archived)
- Integration with Iceberg snapshots for reproducibility

Example:
    >>> registry = ModelRegistry()
    >>> model_version = registry.register_model(
    ...     name="user_ctr_model",
    ...     model_path="/path/to/model.pkl",
    ...     feature_snapshot_id="snap_abc123",
    ...     metrics={"auc": 0.92, "f1": 0.85}
    ... )
    >>> registry.promote_to_production("user_ctr_model", model_version.version)
"""

import os
import json
import shutil
import hashlib
import logging
from enum import Enum
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ModelStage(Enum):
    """Model lifecycle stages."""
    NONE = "none"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


@dataclass
class ModelMetadata:
    """Metadata associated with a model version for lineage tracking."""
    git_commit_hash: Optional[str] = None
    feature_snapshot_id: Optional[str] = None  # Iceberg snapshot ID used for training
    training_run_id: Optional[str] = None
    pipeline_user: Optional[str] = None
    training_data_path: Optional[str] = None
    training_started_at: Optional[str] = None
    training_ended_at: Optional[str] = None
    framework: Optional[str] = None  # pytorch, sklearn, xgboost, etc.
    custom_tags: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class ModelVersion:
    """Represents a specific version of a model."""
    model_name: str
    version: str  # Semantic version: "1.0.0"
    stage: ModelStage
    model_path: str  # Path to serialized model artifact
    model_format: str  # pickle, onnx, torchscript, savedmodel
    metrics: Dict[str, float]
    metadata: ModelMetadata
    created_at: str
    description: Optional[str] = None
    input_schema: Optional[Dict] = None
    output_schema: Optional[Dict] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['stage'] = self.stage.value
        result['metadata'] = self.metadata.to_dict()
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ModelVersion':
        data['stage'] = ModelStage(data['stage'])
        data['metadata'] = ModelMetadata(**data['metadata'])
        return cls(**data)


class ModelRegistry:
    """
    A lightweight, file-based Model Registry for ML model versioning.
    
    In production, this would integrate with:
    - MLflow Model Registry
    - AWS SageMaker Model Registry  
    - Vertex AI Model Registry
    - Custom solutions using S3/GCS + metadata DB
    
    Key Features:
    - Semantic versioning
    - Feature snapshot binding (Iceberg integration)
    - Model lifecycle management
    - Artifact storage
    - Lineage tracking
    """
    
    def __init__(self, registry_path: str = "./model_registry"):
        """
        Initialize the Model Registry.
        
        Args:
            registry_path: Base directory for storing models and metadata
        """
        self.registry_path = Path(registry_path)
        self.metadata_path = self.registry_path / "metadata"
        self.artifacts_path = self.registry_path / "artifacts"
        
        # Create directories
        self.metadata_path.mkdir(parents=True, exist_ok=True)
        self.artifacts_path.mkdir(parents=True, exist_ok=True)
        
        # Index file for quick lookups
        self.index_file = self.registry_path / "registry_index.json"
        self._load_index()
        
        logger.info(f"ModelRegistry initialized at {self.registry_path}")
    
    def _load_index(self):
        """Load the registry index from disk."""
        if self.index_file.exists():
            with open(self.index_file, 'r') as f:
                self._index = json.load(f)
        else:
            self._index = {
                "models": {},  # model_name -> {versions: [], production_version: str, staging_version: str}
                "created_at": datetime.now().isoformat()
            }
            self._save_index()
    
    def _save_index(self):
        """Persist the registry index to disk."""
        with open(self.index_file, 'w') as f:
            json.dump(self._index, f, indent=2)
    
    def _get_next_version(self, model_name: str, bump: str = "patch") -> str:
        """
        Get the next semantic version for a model.
        
        Args:
            model_name: Name of the model
            bump: Version bump type - "major", "minor", or "patch"
        
        Returns:
            Next version string (e.g., "1.0.1")
        """
        if model_name not in self._index["models"]:
            return "1.0.0"
        
        versions = self._index["models"][model_name].get("versions", [])
        if not versions:
            return "1.0.0"
        
        # Get latest version
        latest = sorted(versions, key=lambda v: [int(x) for x in v.split(".")])[-1]
        major, minor, patch = [int(x) for x in latest.split(".")]
        
        if bump == "major":
            return f"{major + 1}.0.0"
        elif bump == "minor":
            return f"{major}.{minor + 1}.0"
        else:
            return f"{major}.{minor}.{patch + 1}"
    
    def _compute_model_hash(self, model_path: str) -> str:
        """Compute SHA256 hash of model artifact."""
        sha256 = hashlib.sha256()
        with open(model_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()[:12]
    
    def register_model(
        self,
        name: str,
        model_path: str,
        metrics: Dict[str, float],
        feature_snapshot_id: Optional[str] = None,
        git_commit_hash: Optional[str] = None,
        training_run_id: Optional[str] = None,
        model_format: str = "pickle",
        description: Optional[str] = None,
        input_schema: Optional[Dict] = None,
        output_schema: Optional[Dict] = None,
        version_bump: str = "patch",
        custom_tags: Optional[Dict[str, str]] = None,
        **extra_metadata
    ) -> ModelVersion:
        """
        Register a new model version in the registry.
        
        Args:
            name: Model name (e.g., "user_ctr_model")
            model_path: Path to the serialized model file
            metrics: Dictionary of evaluation metrics (e.g., {"auc": 0.92})
            feature_snapshot_id: Iceberg snapshot ID used for training data
            git_commit_hash: Git commit of the training code
            training_run_id: Unique identifier for the training run
            model_format: Format of the model (pickle, onnx, torchscript, etc.)
            description: Human-readable description
            input_schema: Expected input schema
            output_schema: Expected output schema
            version_bump: How to bump version ("major", "minor", "patch")
            custom_tags: Additional key-value tags
            **extra_metadata: Additional metadata fields
        
        Returns:
            ModelVersion object representing the registered model
        """
        # Validate model file exists
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model file not found: {model_path}")
        
        # Determine version
        version = self._get_next_version(name, version_bump)
        
        # Create metadata
        metadata = ModelMetadata(
            git_commit_hash=git_commit_hash,
            feature_snapshot_id=feature_snapshot_id,
            training_run_id=training_run_id,
            pipeline_user=extra_metadata.get("pipeline_user"),
            training_data_path=extra_metadata.get("training_data_path"),
            training_started_at=extra_metadata.get("training_started_at"),
            training_ended_at=extra_metadata.get("training_ended_at"),
            framework=extra_metadata.get("framework"),
            custom_tags=custom_tags or {}
        )
        
        # Copy model artifact to registry
        model_hash = self._compute_model_hash(model_path)
        artifact_dir = self.artifacts_path / name / version
        artifact_dir.mkdir(parents=True, exist_ok=True)
        
        model_ext = Path(model_path).suffix
        artifact_path = artifact_dir / f"model_{model_hash}{model_ext}"
        shutil.copy2(model_path, artifact_path)
        
        # Create ModelVersion
        model_version = ModelVersion(
            model_name=name,
            version=version,
            stage=ModelStage.NONE,
            model_path=str(artifact_path),
            model_format=model_format,
            metrics=metrics,
            metadata=metadata,
            created_at=datetime.now().isoformat(),
            description=description,
            input_schema=input_schema,
            output_schema=output_schema
        )
        
        # Save version metadata
        version_meta_path = self.metadata_path / name / f"{version}.json"
        version_meta_path.parent.mkdir(parents=True, exist_ok=True)
        with open(version_meta_path, 'w') as f:
            json.dump(model_version.to_dict(), f, indent=2)
        
        # Update index
        if name not in self._index["models"]:
            self._index["models"][name] = {
                "versions": [],
                "production_version": None,
                "staging_version": None,
                "latest_version": None
            }
        
        self._index["models"][name]["versions"].append(version)
        self._index["models"][name]["latest_version"] = version
        self._save_index()
        
        logger.info(f"Registered model {name}:{version} with metrics {metrics}")
        if feature_snapshot_id:
            logger.info(f"  └── Bound to feature snapshot: {feature_snapshot_id}")
        
        return model_version
    
    def get_model_version(self, name: str, version: str) -> Optional[ModelVersion]:
        """
        Get a specific model version.
        
        Args:
            name: Model name
            version: Version string
        
        Returns:
            ModelVersion object or None if not found
        """
        version_meta_path = self.metadata_path / name / f"{version}.json"
        if not version_meta_path.exists():
            return None
        
        with open(version_meta_path, 'r') as f:
            data = json.load(f)
        
        return ModelVersion.from_dict(data)
    
    def get_production_model(self, name: str) -> Optional[ModelVersion]:
        """Get the production version of a model."""
        if name not in self._index["models"]:
            return None
        
        prod_version = self._index["models"][name].get("production_version")
        if not prod_version:
            return None
        
        return self.get_model_version(name, prod_version)
    
    def get_latest_model(self, name: str) -> Optional[ModelVersion]:
        """Get the latest registered version of a model."""
        if name not in self._index["models"]:
            return None
        
        latest = self._index["models"][name].get("latest_version")
        if not latest:
            return None
        
        return self.get_model_version(name, latest)
    
    def promote_to_staging(self, name: str, version: str) -> ModelVersion:
        """
        Promote a model version to staging.
        
        Args:
            name: Model name
            version: Version to promote
        
        Returns:
            Updated ModelVersion
        """
        model_version = self.get_model_version(name, version)
        if not model_version:
            raise ValueError(f"Model {name}:{version} not found")
        
        # Update stage
        model_version.stage = ModelStage.STAGING
        
        # Demote current staging if exists
        old_staging = self._index["models"][name].get("staging_version")
        if old_staging and old_staging != version:
            old_model = self.get_model_version(name, old_staging)
            if old_model:
                old_model.stage = ModelStage.NONE
                self._save_model_version(old_model)
        
        self._save_model_version(model_version)
        self._index["models"][name]["staging_version"] = version
        self._save_index()
        
        logger.info(f"Promoted {name}:{version} to STAGING")
        return model_version
    
    def promote_to_production(self, name: str, version: str) -> ModelVersion:
        """
        Promote a model version to production.
        
        Args:
            name: Model name
            version: Version to promote
        
        Returns:
            Updated ModelVersion
        """
        model_version = self.get_model_version(name, version)
        if not model_version:
            raise ValueError(f"Model {name}:{version} not found")
        
        # Archive current production
        old_prod = self._index["models"][name].get("production_version")
        if old_prod and old_prod != version:
            old_model = self.get_model_version(name, old_prod)
            if old_model:
                old_model.stage = ModelStage.ARCHIVED
                self._save_model_version(old_model)
                logger.info(f"Archived previous production model {name}:{old_prod}")
        
        # Update stage
        model_version.stage = ModelStage.PRODUCTION
        self._save_model_version(model_version)
        
        self._index["models"][name]["production_version"] = version
        # Clear staging if same version
        if self._index["models"][name].get("staging_version") == version:
            self._index["models"][name]["staging_version"] = None
        self._save_index()
        
        logger.info(f"🚀 Promoted {name}:{version} to PRODUCTION")
        return model_version
    
    def _save_model_version(self, model_version: ModelVersion):
        """Save model version metadata to disk."""
        version_meta_path = self.metadata_path / model_version.model_name / f"{model_version.version}.json"
        with open(version_meta_path, 'w') as f:
            json.dump(model_version.to_dict(), f, indent=2)
    
    def list_models(self) -> List[str]:
        """List all registered model names."""
        return list(self._index["models"].keys())
    
    def list_versions(self, name: str) -> List[Dict]:
        """
        List all versions of a model with their stages and key metrics.
        
        Args:
            name: Model name
        
        Returns:
            List of version summary dicts
        """
        if name not in self._index["models"]:
            return []
        
        result = []
        for version in self._index["models"][name]["versions"]:
            mv = self.get_model_version(name, version)
            if mv:
                result.append({
                    "version": mv.version,
                    "stage": mv.stage.value,
                    "metrics": mv.metrics,
                    "feature_snapshot": mv.metadata.feature_snapshot_id,
                    "created_at": mv.created_at
                })
        
        return result
    
    def compare_versions(self, name: str, version_a: str, version_b: str) -> Dict:
        """
        Compare two model versions.
        
        Args:
            name: Model name
            version_a: First version
            version_b: Second version
        
        Returns:
            Comparison dict with metrics diff
        """
        mv_a = self.get_model_version(name, version_a)
        mv_b = self.get_model_version(name, version_b)
        
        if not mv_a or not mv_b:
            raise ValueError("One or both versions not found")
        
        # Compute metric diffs
        all_metrics = set(mv_a.metrics.keys()) | set(mv_b.metrics.keys())
        metric_comparison = {}
        
        for metric in all_metrics:
            val_a = mv_a.metrics.get(metric)
            val_b = mv_b.metrics.get(metric)
            
            if val_a is not None and val_b is not None:
                diff = val_b - val_a
                pct_change = (diff / val_a * 100) if val_a != 0 else 0
                metric_comparison[metric] = {
                    f"{version_a}": val_a,
                    f"{version_b}": val_b,
                    "diff": round(diff, 4),
                    "pct_change": round(pct_change, 2)
                }
        
        return {
            "model_name": name,
            "versions_compared": [version_a, version_b],
            "metrics": metric_comparison,
            "feature_snapshots": {
                version_a: mv_a.metadata.feature_snapshot_id,
                version_b: mv_b.metadata.feature_snapshot_id
            }
        }
    
    def get_model_lineage(self, name: str, version: str) -> Dict:
        """
        Get complete lineage information for a model version.
        
        Useful for debugging and reproducibility.
        """
        mv = self.get_model_version(name, version)
        if not mv:
            raise ValueError(f"Model {name}:{version} not found")
        
        return {
            "model": f"{name}:{version}",
            "created_at": mv.created_at,
            "code_lineage": {
                "git_commit": mv.metadata.git_commit_hash,
                "training_run_id": mv.metadata.training_run_id,
                "pipeline_user": mv.metadata.pipeline_user
            },
            "data_lineage": {
                "feature_snapshot_id": mv.metadata.feature_snapshot_id,
                "training_data_path": mv.metadata.training_data_path,
                "iceberg_query": f"SELECT * FROM features FOR SYSTEM_VERSION AS OF {mv.metadata.feature_snapshot_id}" if mv.metadata.feature_snapshot_id else None
            },
            "environment": {
                "framework": mv.metadata.framework
            },
            "artifact": {
                "path": mv.model_path,
                "format": mv.model_format
            },
            "metrics": mv.metrics,
            "custom_tags": mv.metadata.custom_tags
        }


# Convenience functions for integration with existing codebase
def get_git_commit_for_registry():
    """Get current git commit hash for model registration."""
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    try:
        from src.common.git_utils import get_git_commit_hash
        return get_git_commit_hash()
    except:
        return None


if __name__ == "__main__":
    # Demo usage
    import tempfile
    import pickle
    
    # Create a mock model
    mock_model = {"type": "mock", "weights": [1, 2, 3]}
    model_file = tempfile.NamedTemporaryFile(suffix=".pkl", delete=False)
    pickle.dump(mock_model, model_file)
    model_file.close()
    
    # Initialize registry
    registry = ModelRegistry("./model_registry")
    
    # Register model with feature lineage
    v1 = registry.register_model(
        name="user_ctr_model",
        model_path=model_file.name,
        metrics={"auc": 0.85, "f1": 0.78, "precision": 0.82},
        feature_snapshot_id="3847293847293",
        git_commit_hash="abc123def",
        training_run_id="run_001",
        framework="sklearn",
        description="Initial CTR model"
    )
    print(f"\n✅ Registered: {v1.model_name}:{v1.version}")
    
    # Register another version
    v2 = registry.register_model(
        name="user_ctr_model",
        model_path=model_file.name,
        metrics={"auc": 0.92, "f1": 0.85, "precision": 0.88},
        feature_snapshot_id="9283749283749",
        git_commit_hash="def456ghi",
        training_run_id="run_002",
        framework="sklearn",
        description="Improved CTR model with new features"
    )
    print(f"✅ Registered: {v2.model_name}:{v2.version}")
    
    # Compare versions
    print("\n📊 Version Comparison:")
    comparison = registry.compare_versions("user_ctr_model", "1.0.0", "1.0.1")
    print(json.dumps(comparison, indent=2))
    
    # Promote to production
    registry.promote_to_production("user_ctr_model", "1.0.1")
    
    # Get lineage
    print("\n🔗 Model Lineage:")
    lineage = registry.get_model_lineage("user_ctr_model", "1.0.1")
    print(json.dumps(lineage, indent=2))
    
    # Cleanup
    os.unlink(model_file.name)
