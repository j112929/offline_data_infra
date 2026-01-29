"""
Alert Management Module - Threshold-based alerting for ML pipelines.

This module provides:
- Configurable alert rules
- Multiple notification channels (Slack, PagerDuty, Email)
- Alert severity levels
- Alert history and deduplication
- Integration with monitoring metrics

Example:
    >>> manager = AlertManager()
    >>> manager.add_rule(AlertRule(
    ...     name="high_null_rate",
    ...     condition=lambda m: m.get_gauge("feature_null_rate") > 0.1,
    ...     severity=AlertSeverity.WARNING,
    ...     message="Feature null rate exceeds 10%"
    ... ))
    >>> manager.evaluate_all(metrics_collector)
"""

import logging
import time
import json
import os
from enum import Enum
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Callable, Any
from collections import defaultdict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    
    def emoji(self) -> str:
        """Get emoji for severity level."""
        return {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "🔴",
            "critical": "🚨"
        }.get(self.value, "📢")


class AlertStatus(Enum):
    """Alert lifecycle status."""
    FIRING = "firing"
    RESOLVED = "resolved"
    SILENCED = "silenced"


@dataclass
class AlertRule:
    """
    Definition of an alert rule.
    
    Attributes:
        name: Unique identifier for the rule
        condition: Function that returns True if alert should fire
        severity: Severity level of the alert
        message: Human-readable alert message
        description: Detailed description for runbooks
        labels: Additional labels for grouping/routing
        for_duration: How long condition must be true before firing (seconds)
        cooldown: Minimum time between repeated alerts (seconds)
    """
    name: str
    condition: Callable[['MetricsCollector'], bool]
    severity: AlertSeverity
    message: str
    description: str = ""
    labels: Dict[str, str] = field(default_factory=dict)
    for_duration: float = 0  # Fire immediately by default
    cooldown: float = 300  # 5 minutes default cooldown
    runbook_url: str = ""


@dataclass
class Alert:
    """An active or historical alert instance."""
    rule_name: str
    severity: AlertSeverity
    status: AlertStatus
    message: str
    description: str
    labels: Dict[str, str]
    fired_at: str
    resolved_at: Optional[str] = None
    acknowledged_by: Optional[str] = None
    fingerprint: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['severity'] = self.severity.value
        result['status'] = self.status.value
        return result


class NotificationChannel:
    """Base class for notification channels."""
    
    def send(self, alert: Alert) -> bool:
        """Send alert notification. Returns True if successful."""
        raise NotImplementedError


class SlackNotifier(NotificationChannel):
    """Slack webhook notification channel."""
    
    def __init__(self, webhook_url: str, channel: str = "#alerts"):
        self.webhook_url = webhook_url
        self.channel = channel
    
    def send(self, alert: Alert) -> bool:
        """Send alert to Slack."""
        try:
            import urllib.request
            
            color = {
                AlertSeverity.INFO: "#36a64f",
                AlertSeverity.WARNING: "#ffcc00",
                AlertSeverity.ERROR: "#ff6600",
                AlertSeverity.CRITICAL: "#ff0000"
            }.get(alert.severity, "#808080")
            
            payload = {
                "channel": self.channel,
                "username": "ML Platform Alerts",
                "icon_emoji": ":robot_face:",
                "attachments": [{
                    "color": color,
                    "title": f"{alert.severity.emoji()} {alert.rule_name}",
                    "text": alert.message,
                    "fields": [
                        {"title": "Severity", "value": alert.severity.value, "short": True},
                        {"title": "Status", "value": alert.status.value, "short": True},
                        {"title": "Fired At", "value": alert.fired_at, "short": True}
                    ],
                    "footer": "ML Platform Alerting",
                    "ts": time.time()
                }]
            }
            
            if alert.description:
                payload["attachments"][0]["fields"].append({
                    "title": "Description",
                    "value": alert.description,
                    "short": False
                })
            
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                self.webhook_url,
                data=data,
                headers={'Content-Type': 'application/json'}
            )
            
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status == 200
                
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False


class ConsoleNotifier(NotificationChannel):
    """Console/log notification channel for development."""
    
    def send(self, alert: Alert) -> bool:
        """Print alert to console."""
        separator = "=" * 60
        logger.info(f"\n{separator}")
        logger.info(f"{alert.severity.emoji()} ALERT: {alert.rule_name}")
        logger.info(f"Severity: {alert.severity.value.upper()}")
        logger.info(f"Status: {alert.status.value}")
        logger.info(f"Message: {alert.message}")
        if alert.description:
            logger.info(f"Description: {alert.description}")
        logger.info(f"Fired At: {alert.fired_at}")
        logger.info(f"Labels: {alert.labels}")
        logger.info(separator)
        return True


class FileNotifier(NotificationChannel):
    """File-based notification for audit trail."""
    
    def __init__(self, filepath: str = "./alerts.jsonl"):
        self.filepath = filepath
    
    def send(self, alert: Alert) -> bool:
        """Append alert to file."""
        try:
            with open(self.filepath, 'a') as f:
                f.write(json.dumps(alert.to_dict()) + "\n")
            return True
        except Exception as e:
            logger.error(f"Failed to write alert to file: {e}")
            return False


class AlertManager:
    """
    Central alert management for ML platform.
    
    Features:
    - Rule-based alerting
    - Multiple notification channels
    - Alert deduplication
    - Silencing support
    - Alert history
    """
    
    def __init__(
        self,
        notifiers: List[NotificationChannel] = None,
        history_file: str = "./alert_history.jsonl"
    ):
        """
        Initialize the alert manager.
        
        Args:
            notifiers: List of notification channels
            history_file: Path to store alert history
        """
        self.rules: Dict[str, AlertRule] = {}
        self.notifiers = notifiers or [ConsoleNotifier()]
        self.history_file = history_file
        
        # Track rule state
        self._condition_first_true: Dict[str, float] = {}  # rule_name -> timestamp
        self._last_fired: Dict[str, float] = {}  # rule_name -> timestamp
        self._active_alerts: Dict[str, Alert] = {}  # rule_name -> Alert
        self._silenced_rules: Dict[str, float] = {}  # rule_name -> until_timestamp
        
        logger.info("AlertManager initialized")
    
    def add_rule(self, rule: AlertRule):
        """Add an alert rule."""
        self.rules[rule.name] = rule
        logger.info(f"Added alert rule: {rule.name} ({rule.severity.value})")
    
    def remove_rule(self, rule_name: str):
        """Remove an alert rule."""
        if rule_name in self.rules:
            del self.rules[rule_name]
            logger.info(f"Removed alert rule: {rule_name}")
    
    def silence_rule(self, rule_name: str, duration_seconds: float = 3600):
        """Silence a rule for a specified duration."""
        until = time.time() + duration_seconds
        self._silenced_rules[rule_name] = until
        logger.info(f"Silenced rule {rule_name} until {datetime.fromtimestamp(until).isoformat()}")
    
    def unsilence_rule(self, rule_name: str):
        """Unsilence a rule."""
        if rule_name in self._silenced_rules:
            del self._silenced_rules[rule_name]
            logger.info(f"Unsilenced rule {rule_name}")
    
    def _is_silenced(self, rule_name: str) -> bool:
        """Check if a rule is currently silenced."""
        if rule_name not in self._silenced_rules:
            return False
        if time.time() > self._silenced_rules[rule_name]:
            del self._silenced_rules[rule_name]
            return False
        return True
    
    def _generate_fingerprint(self, rule: AlertRule, labels: Dict[str, str]) -> str:
        """Generate unique fingerprint for alert deduplication."""
        import hashlib
        content = f"{rule.name}:{json.dumps(labels, sort_keys=True)}"
        return hashlib.md5(content.encode()).hexdigest()[:12]
    
    def evaluate_rule(self, rule: AlertRule, metrics) -> Optional[Alert]:
        """
        Evaluate a single rule against metrics.
        
        Args:
            rule: AlertRule to evaluate
            metrics: MetricsCollector instance
        
        Returns:
            Alert if rule fires, None otherwise
        """
        now = time.time()
        
        # Check if silenced
        if self._is_silenced(rule.name):
            return None
        
        # Evaluate condition
        try:
            condition_met = rule.condition(metrics)
        except Exception as e:
            logger.error(f"Error evaluating rule {rule.name}: {e}")
            return None
        
        if condition_met:
            # Track when condition first became true
            if rule.name not in self._condition_first_true:
                self._condition_first_true[rule.name] = now
            
            first_true = self._condition_first_true[rule.name]
            duration_met = (now - first_true) >= rule.for_duration
            
            # Check cooldown
            last_fired = self._last_fired.get(rule.name, 0)
            cooldown_passed = (now - last_fired) >= rule.cooldown
            
            if duration_met and cooldown_passed:
                # Fire alert
                alert = Alert(
                    rule_name=rule.name,
                    severity=rule.severity,
                    status=AlertStatus.FIRING,
                    message=rule.message,
                    description=rule.description,
                    labels=rule.labels,
                    fired_at=datetime.now().isoformat(),
                    fingerprint=self._generate_fingerprint(rule, rule.labels)
                )
                
                self._last_fired[rule.name] = now
                self._active_alerts[rule.name] = alert
                
                return alert
        else:
            # Condition no longer met
            if rule.name in self._condition_first_true:
                del self._condition_first_true[rule.name]
            
            # Resolve active alert if exists
            if rule.name in self._active_alerts:
                active = self._active_alerts[rule.name]
                active.status = AlertStatus.RESOLVED
                active.resolved_at = datetime.now().isoformat()
                self._save_to_history(active)
                del self._active_alerts[rule.name]
                logger.info(f"Alert {rule.name} resolved")
        
        return None
    
    def evaluate_all(self, metrics) -> List[Alert]:
        """
        Evaluate all rules against current metrics.
        
        Args:
            metrics: MetricsCollector instance
        
        Returns:
            List of newly fired alerts
        """
        fired_alerts = []
        
        for rule in self.rules.values():
            alert = self.evaluate_rule(rule, metrics)
            if alert:
                fired_alerts.append(alert)
                self._notify_all(alert)
                self._save_to_history(alert)
        
        return fired_alerts
    
    def _notify_all(self, alert: Alert):
        """Send alert to all notification channels."""
        for notifier in self.notifiers:
            try:
                notifier.send(alert)
            except Exception as e:
                logger.error(f"Failed to send notification via {type(notifier).__name__}: {e}")
    
    def _save_to_history(self, alert: Alert):
        """Save alert to history file."""
        try:
            with open(self.history_file, 'a') as f:
                f.write(json.dumps(alert.to_dict()) + "\n")
        except Exception as e:
            logger.error(f"Failed to save alert to history: {e}")
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all currently active alerts."""
        return list(self._active_alerts.values())
    
    def get_alert_history(self, limit: int = 100) -> List[Dict]:
        """Load recent alert history."""
        if not os.path.exists(self.history_file):
            return []
        
        alerts = []
        try:
            with open(self.history_file, 'r') as f:
                for line in f:
                    if line.strip():
                        alerts.append(json.loads(line))
        except Exception as e:
            logger.error(f"Failed to load alert history: {e}")
        
        return alerts[-limit:]
    
    def get_summary(self) -> Dict[str, Any]:
        """Get alert summary."""
        history = self.get_alert_history(1000)
        
        # Count by severity
        by_severity = defaultdict(int)
        for alert in history:
            by_severity[alert.get('severity', 'unknown')] += 1
        
        # Count active
        active_by_severity = defaultdict(int)
        for alert in self._active_alerts.values():
            active_by_severity[alert.severity.value] += 1
        
        return {
            "total_rules": len(self.rules),
            "active_alerts": len(self._active_alerts),
            "silenced_rules": len(self._silenced_rules),
            "history_count": len(history),
            "by_severity": dict(by_severity),
            "active_by_severity": dict(active_by_severity),
            "notifiers": [type(n).__name__ for n in self.notifiers]
        }


# ==================== Pre-built Alert Rules ====================

def create_default_alert_rules() -> List[AlertRule]:
    """Create default alert rules for ML platform."""
    return [
        AlertRule(
            name="high_feature_null_rate",
            condition=lambda m: any(
                v > 0.1 for k, v in m._gauges.items() if "null_rate" in k
            ),
            severity=AlertSeverity.WARNING,
            message="Feature null rate exceeds 10%",
            description="One or more features have high null rates, which may indicate data quality issues.",
            for_duration=60,
            cooldown=300,
            runbook_url="https://docs.example.com/runbooks/high-null-rate"
        ),
        
        AlertRule(
            name="prediction_latency_high",
            condition=lambda m: (
                "prediction_latency_seconds" in m._histograms and
                m._histograms["prediction_latency_seconds"].get_percentile(0.99) > 0.5
            ),
            severity=AlertSeverity.ERROR,
            message="P99 prediction latency exceeds 500ms",
            description="Prediction latency is degraded. Check model serving infrastructure.",
            for_duration=120,
            cooldown=600,
            labels={"team": "ml-infra"}
        ),
        
        AlertRule(
            name="job_failure_spike",
            condition=lambda m: (
                sum(v for k, v in m._counters.items() if "failure" in k) > 3
            ),
            severity=AlertSeverity.CRITICAL,
            message="Multiple job failures detected",
            description="More than 3 job failures recorded. Immediate investigation required.",
            for_duration=0,
            cooldown=900,
            labels={"team": "data-eng", "oncall": "true"}
        ),
        
        AlertRule(
            name="feature_drift_detected",
            condition=lambda m: any(
                v > 0 for k, v in m._gauges.items() if "drift_detected" in k
            ),
            severity=AlertSeverity.WARNING,
            message="Feature distribution drift detected",
            description="One or more features show significant distribution drift from training data.",
            for_duration=300,
            cooldown=3600
        ),
        
        AlertRule(
            name="model_too_old",
            condition=lambda m: any(
                v > 168 for k, v in m._gauges.items() if "model_age_hours" in k  # > 1 week
            ),
            severity=AlertSeverity.INFO,
            message="Production model is over 1 week old",
            description="Consider retraining the model with recent data.",
            for_duration=0,
            cooldown=86400  # Alert once per day
        ),
        
        AlertRule(
            name="online_store_empty",
            condition=lambda m: any(
                v == 0 for k, v in m._gauges.items() if "online_store_keys" in k
            ),
            severity=AlertSeverity.CRITICAL,
            message="Online feature store is empty",
            description="No features in online store. Serving will fail.",
            for_duration=0,
            cooldown=300
        )
    ]


def setup_default_alerting(
    metrics_collector,
    slack_webhook: Optional[str] = None
) -> AlertManager:
    """
    Set up default alerting with standard rules.
    
    Args:
        metrics_collector: MetricsCollector instance
        slack_webhook: Optional Slack webhook URL
    
    Returns:
        Configured AlertManager
    """
    notifiers = [ConsoleNotifier(), FileNotifier()]
    
    if slack_webhook:
        notifiers.append(SlackNotifier(slack_webhook))
    
    manager = AlertManager(notifiers=notifiers)
    
    for rule in create_default_alert_rules():
        manager.add_rule(rule)
    
    return manager


if __name__ == "__main__":
    # Demo usage
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    
    from src.monitoring.metrics import MetricsCollector
    
    # Create metrics collector
    collector = MetricsCollector("demo_service")
    
    # Simulate some metrics
    collector.record_feature_null_rate("user_id", 0.15)  # High null rate!
    collector.record_job_failure("backfill", "timeout")
    collector.record_job_failure("sync", "connection_error")
    collector.record_job_failure("maintenance", "permission_denied")
    collector.record_job_failure("prediction", "model_not_found")
    
    # Set up alerting
    manager = AlertManager(notifiers=[ConsoleNotifier()])
    
    # Add default rules
    for rule in create_default_alert_rules():
        manager.add_rule(rule)
    
    # Evaluate rules
    print("\n🔍 Evaluating Alert Rules...")
    fired = manager.evaluate_all(collector)
    
    print(f"\n📊 Fired {len(fired)} alerts")
    
    # Show summary
    print("\n📋 Alert Summary:")
    print(json.dumps(manager.get_summary(), indent=2))
