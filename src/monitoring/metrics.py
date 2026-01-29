"""
Metrics Collection Module - Production-grade observability for ML pipelines.

This module provides:
- Prometheus-compatible metrics export
- Data quality metrics tracking
- Prediction distribution monitoring
- Latency and throughput metrics
- Integration with Grafana dashboards

Example:
    >>> collector = MetricsCollector()
    >>> collector.record_prediction_latency(0.05)
    >>> collector.record_feature_null_rate("user_id", 0.01)
    >>> collector.export_prometheus()
"""

import time
import logging
import threading
from enum import Enum
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from collections import defaultdict
import json
import os

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics supported."""
    COUNTER = "counter"       # Monotonically increasing (e.g., request count)
    GAUGE = "gauge"           # Point-in-time value (e.g., current queue size)
    HISTOGRAM = "histogram"   # Distribution of values (e.g., latency)
    SUMMARY = "summary"       # Similar to histogram with quantiles


@dataclass
class MetricValue:
    """A single metric value with metadata."""
    name: str
    value: float
    metric_type: MetricType
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    description: str = ""
    
    def to_prometheus_format(self) -> str:
        """Convert to Prometheus text format."""
        label_str = ",".join(f'{k}="{v}"' for k, v in self.labels.items())
        if label_str:
            return f"{self.name}{{{label_str}}} {self.value}"
        return f"{self.name} {self.value}"


@dataclass
class HistogramBucket:
    """Histogram bucket for distribution tracking."""
    le: float  # Less than or equal
    count: int = 0


class Histogram:
    """Histogram implementation for tracking distributions."""
    
    # Default buckets for latency (in seconds)
    DEFAULT_LATENCY_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    
    def __init__(self, name: str, description: str = "", buckets: List[float] = None):
        self.name = name
        self.description = description
        self.buckets = sorted(buckets or self.DEFAULT_LATENCY_BUCKETS)
        self._bucket_counts = {b: 0 for b in self.buckets}
        self._bucket_counts[float('inf')] = 0  # +Inf bucket
        self._sum = 0.0
        self._count = 0
        self._lock = threading.Lock()
    
    def observe(self, value: float):
        """Record a value in the histogram."""
        with self._lock:
            self._sum += value
            self._count += 1
            for bucket in self.buckets:
                if value <= bucket:
                    self._bucket_counts[bucket] += 1
            self._bucket_counts[float('inf')] += 1
    
    def get_percentile(self, p: float) -> float:
        """Estimate percentile value (approximate)."""
        if self._count == 0:
            return 0.0
        
        target_count = p * self._count
        cumulative = 0
        prev_bucket = 0
        
        for bucket in self.buckets:
            cumulative = self._bucket_counts[bucket]
            if cumulative >= target_count:
                # Linear interpolation
                return (prev_bucket + bucket) / 2
            prev_bucket = bucket
        
        return self.buckets[-1]
    
    def to_prometheus_format(self, labels: Dict[str, str] = None) -> List[str]:
        """Export in Prometheus format."""
        lines = []
        label_str = ",".join(f'{k}="{v}"' for k, v in (labels or {}).items())
        
        cumulative = 0
        for bucket in self.buckets + [float('inf')]:
            cumulative = self._bucket_counts[bucket]
            le = "+Inf" if bucket == float('inf') else bucket
            if label_str:
                lines.append(f'{self.name}_bucket{{le="{le}",{label_str}}} {cumulative}')
            else:
                lines.append(f'{self.name}_bucket{{le="{le}"}} {cumulative}')
        
        lines.append(f'{self.name}_sum {self._sum}')
        lines.append(f'{self.name}_count {self._count}')
        
        return lines


class MetricsCollector:
    """
    Central metrics collection for ML platform observability.
    
    Collects and exports metrics for:
    - Prediction latency and throughput
    - Feature quality (null rates, distribution drift)
    - Model performance (accuracy, AUC over time)
    - Pipeline health (job success/failure)
    
    Integration:
    - Prometheus: /metrics endpoint
    - Grafana: Pre-built dashboard templates
    - Alerting: Threshold-based alerts
    """
    
    def __init__(self, service_name: str = "offline_feature_platform"):
        """
        Initialize the metrics collector.
        
        Args:
            service_name: Name of the service for metric labeling
        """
        self.service_name = service_name
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._labels: Dict[str, Dict[str, str]] = {}
        self._lock = threading.Lock()
        
        # Initialize default histograms
        self._histograms["prediction_latency_seconds"] = Histogram(
            "prediction_latency_seconds",
            "Prediction latency in seconds"
        )
        self._histograms["feature_sync_duration_seconds"] = Histogram(
            "feature_sync_duration_seconds",
            "Feature sync job duration"
        )
        self._histograms["batch_prediction_duration_seconds"] = Histogram(
            "batch_prediction_duration_seconds",
            "Batch prediction job duration"
        )
        
        logger.info(f"MetricsCollector initialized for service: {service_name}")
    
    # ==================== Counter Methods ====================
    
    def increment_counter(self, name: str, value: float = 1.0, labels: Dict[str, str] = None):
        """Increment a counter metric."""
        key = self._make_key(name, labels)
        with self._lock:
            self._counters[key] += value
            if labels:
                self._labels[key] = labels
    
    def record_prediction_count(self, count: int = 1, model_name: str = None):
        """Record number of predictions made."""
        labels = {"model": model_name} if model_name else {}
        self.increment_counter("predictions_total", count, labels)
    
    def record_job_success(self, job_type: str):
        """Record a successful job execution."""
        self.increment_counter("job_success_total", 1, {"job_type": job_type})
    
    def record_job_failure(self, job_type: str, error_type: str = "unknown"):
        """Record a failed job execution."""
        self.increment_counter("job_failure_total", 1, {
            "job_type": job_type,
            "error_type": error_type
        })
    
    # ==================== Gauge Methods ====================
    
    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        """Set a gauge metric to a specific value."""
        key = self._make_key(name, labels)
        with self._lock:
            self._gauges[key] = value
            if labels:
                self._labels[key] = labels
    
    def record_feature_null_rate(self, feature_name: str, null_rate: float):
        """Record null rate for a feature."""
        self.set_gauge("feature_null_rate", null_rate, {"feature": feature_name})
    
    def record_feature_mean(self, feature_name: str, mean_value: float):
        """Record mean value of a feature."""
        self.set_gauge("feature_mean", mean_value, {"feature": feature_name})
    
    def record_feature_stddev(self, feature_name: str, stddev: float):
        """Record standard deviation of a feature."""
        self.set_gauge("feature_stddev", stddev, {"feature": feature_name})
    
    def record_prediction_distribution(self, mean: float, stddev: float, model_name: str):
        """Record prediction distribution stats."""
        self.set_gauge("prediction_mean", mean, {"model": model_name})
        self.set_gauge("prediction_stddev", stddev, {"model": model_name})
    
    def record_online_store_size(self, size: int, store_type: str = "redis"):
        """Record online store size (number of keys)."""
        self.set_gauge("online_store_keys", size, {"store_type": store_type})
    
    def record_model_freshness(self, model_name: str, hours_since_training: float):
        """Record how old the production model is."""
        self.set_gauge("model_age_hours", hours_since_training, {"model": model_name})
    
    # ==================== Histogram Methods ====================
    
    def record_latency(self, histogram_name: str, duration_seconds: float, labels: Dict[str, str] = None):
        """Record a latency observation."""
        key = histogram_name
        if key not in self._histograms:
            self._histograms[key] = Histogram(key)
        self._histograms[key].observe(duration_seconds)
    
    def record_prediction_latency(self, duration_seconds: float, model_name: str = None):
        """Record prediction latency."""
        self._histograms["prediction_latency_seconds"].observe(duration_seconds)
        if model_name:
            key = f"prediction_latency_seconds_{model_name}"
            if key not in self._histograms:
                self._histograms[key] = Histogram(key)
            self._histograms[key].observe(duration_seconds)
    
    def record_sync_duration(self, duration_seconds: float):
        """Record online sync job duration."""
        self._histograms["feature_sync_duration_seconds"].observe(duration_seconds)
    
    def record_batch_prediction_duration(self, duration_seconds: float, model_name: str = None):
        """Record batch prediction job duration."""
        self._histograms["batch_prediction_duration_seconds"].observe(duration_seconds)
    
    # ==================== Data Quality Metrics ====================
    
    def record_data_quality_check(
        self,
        table_name: str,
        check_name: str,
        passed: bool,
        value: Optional[float] = None
    ):
        """Record a data quality check result."""
        self.increment_counter("data_quality_checks_total", 1, {
            "table": table_name,
            "check": check_name,
            "result": "passed" if passed else "failed"
        })
        
        if value is not None:
            self.set_gauge(f"data_quality_{check_name}", value, {"table": table_name})
    
    def record_schema_evolution(self, table_name: str, change_type: str):
        """Record schema evolution events."""
        self.increment_counter("schema_changes_total", 1, {
            "table": table_name,
            "change_type": change_type  # add_column, drop_column, rename, etc.
        })
    
    # ==================== Drift Detection Metrics ====================
    
    def record_feature_drift(
        self,
        feature_name: str,
        drift_score: float,
        drift_detected: bool
    ):
        """Record feature drift detection results."""
        self.set_gauge("feature_drift_score", drift_score, {"feature": feature_name})
        self.set_gauge("feature_drift_detected", 1.0 if drift_detected else 0.0, {"feature": feature_name})
    
    def record_prediction_drift(
        self,
        model_name: str,
        drift_score: float,
        drift_detected: bool
    ):
        """Record prediction distribution drift."""
        self.set_gauge("prediction_drift_score", drift_score, {"model": model_name})
        self.set_gauge("prediction_drift_detected", 1.0 if drift_detected else 0.0, {"model": model_name})
    
    # ==================== Export Methods ====================
    
    def _make_key(self, name: str, labels: Dict[str, str] = None) -> str:
        """Create a unique key for a metric with labels."""
        if not labels:
            return name
        label_str = "_".join(f"{k}_{v}" for k, v in sorted(labels.items()))
        return f"{name}_{label_str}"
    
    def export_prometheus(self) -> str:
        """
        Export all metrics in Prometheus text format.
        
        Returns:
            Prometheus-compatible metrics string
        """
        lines = []
        lines.append(f"# HELP service_info Service information")
        lines.append(f"# TYPE service_info gauge")
        lines.append(f'service_info{{service="{self.service_name}"}} 1')
        lines.append("")
        
        # Export counters
        lines.append("# TYPE predictions_total counter")
        for key, value in self._counters.items():
            labels = self._labels.get(key, {})
            metric_name = key.split("_")[0] if "_" in key else key
            if labels:
                label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
                lines.append(f"{key.split('_')[0]}_total{{{label_str}}} {value}")
            else:
                lines.append(f"{key} {value}")
        lines.append("")
        
        # Export gauges
        for key, value in self._gauges.items():
            labels = self._labels.get(key, {})
            if labels:
                label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
                base_name = key.split("_feature_")[0] if "_feature_" in key else key.split("_model_")[0] if "_model_" in key else key
                lines.append(f"{base_name}{{{label_str}}} {value}")
            else:
                lines.append(f"{key} {value}")
        lines.append("")
        
        # Export histograms
        for name, histogram in self._histograms.items():
            lines.append(f"# TYPE {name} histogram")
            lines.extend(histogram.to_prometheus_format())
            lines.append("")
        
        return "\n".join(lines)
    
    def export_json(self) -> Dict[str, Any]:
        """Export metrics as JSON."""
        result = {
            "service": self.service_name,
            "timestamp": datetime.now().isoformat(),
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {}
        }
        
        for name, hist in self._histograms.items():
            result["histograms"][name] = {
                "count": hist._count,
                "sum": hist._sum,
                "mean": hist._sum / hist._count if hist._count > 0 else 0,
                "p50": hist.get_percentile(0.5),
                "p95": hist.get_percentile(0.95),
                "p99": hist.get_percentile(0.99)
            }
        
        return result
    
    def save_metrics(self, filepath: str = "./metrics_snapshot.json"):
        """Save metrics snapshot to file."""
        with open(filepath, 'w') as f:
            json.dump(self.export_json(), f, indent=2)
        logger.info(f"Metrics saved to {filepath}")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of key metrics."""
        summary = {
            "service": self.service_name,
            "prediction_latency": {},
            "data_quality": {},
            "job_stats": {}
        }
        
        # Latency summary
        if "prediction_latency_seconds" in self._histograms:
            hist = self._histograms["prediction_latency_seconds"]
            summary["prediction_latency"] = {
                "count": hist._count,
                "mean_ms": (hist._sum / hist._count * 1000) if hist._count > 0 else 0,
                "p50_ms": hist.get_percentile(0.5) * 1000,
                "p99_ms": hist.get_percentile(0.99) * 1000
            }
        
        # Job stats
        success_keys = [k for k in self._counters if "success" in k]
        failure_keys = [k for k in self._counters if "failure" in k]
        
        summary["job_stats"]["total_success"] = sum(self._counters[k] for k in success_keys)
        summary["job_stats"]["total_failure"] = sum(self._counters[k] for k in failure_keys)
        
        return summary


# Singleton instance for easy access
_default_collector: Optional[MetricsCollector] = None


def get_metrics_collector(service_name: str = "offline_feature_platform") -> MetricsCollector:
    """Get or create the default metrics collector."""
    global _default_collector
    if _default_collector is None:
        _default_collector = MetricsCollector(service_name)
    return _default_collector


# Context manager for timing operations
class Timer:
    """Context manager for timing code blocks."""
    
    def __init__(self, collector: MetricsCollector, metric_name: str, labels: Dict[str, str] = None):
        self.collector = collector
        self.metric_name = metric_name
        self.labels = labels
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        self.collector.record_latency(self.metric_name, duration, self.labels)


if __name__ == "__main__":
    # Demo usage
    collector = MetricsCollector("demo_service")
    
    # Record some metrics
    for i in range(100):
        collector.record_prediction_count(1, "user_ctr_model")
        collector.record_prediction_latency(0.01 + (i % 10) * 0.005, "user_ctr_model")
    
    collector.record_feature_null_rate("user_id", 0.001)
    collector.record_feature_null_rate("total_value", 0.05)
    collector.record_feature_mean("total_value", 125.5)
    collector.record_feature_drift("total_value", 0.15, False)
    
    collector.record_job_success("backfill")
    collector.record_job_success("backfill")
    collector.record_job_failure("sync", "timeout")
    
    # Export
    print("=" * 60)
    print("PROMETHEUS FORMAT:")
    print("=" * 60)
    print(collector.export_prometheus())
    
    print("\n" + "=" * 60)
    print("JSON FORMAT:")
    print("=" * 60)
    print(json.dumps(collector.export_json(), indent=2))
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print("=" * 60)
    print(json.dumps(collector.get_summary(), indent=2))
