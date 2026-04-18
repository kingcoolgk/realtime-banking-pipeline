"""
dq/dq_alert.py
----------------
Alerting module for DQ failures.
Supports logging + extensible hooks for Slack / email / PagerDuty.
"""

import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def send_dq_alert(batch_id: int, alert_type: str,
                   details: dict, severity: str = "ERROR"):
    """
    Send a DQ alert. Currently logs to console.
    Extend this to push to Slack webhook, SNS, or email as needed.

    Args:
        batch_id   : Spark micro-batch ID
        alert_type : e.g. SCHEMA_DRIFT, NULL_BREACH, ANOMALY_FLAG
        details    : dict with check-specific metadata
        severity   : ERROR (hard fail) | WARNING (soft flag)
    """
    alert_payload = {
        "timestamp":  datetime.utcnow().isoformat(),
        "batch_id":   batch_id,
        "alert_type": alert_type,
        "severity":   severity,
        "details":    details,
    }

    log_msg = f"[DQ ALERT | {severity}] {alert_type} — Batch {batch_id} | {json.dumps(details)}"

    if severity == "ERROR":
        logger.error(log_msg)
    else:
        logger.warning(log_msg)

    # ── Extension point ───────────────────────────────────────────────────────
    # Uncomment and configure to enable Slack notifications:
    #
    # import requests
    # SLACK_WEBHOOK = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    # requests.post(SLACK_WEBHOOK, json={"text": f":red_circle: {log_msg}"})
    #
    # Or AWS SNS:
    # import boto3
    # sns = boto3.client("sns", region_name="ap-south-1")
    # sns.publish(TopicArn="arn:aws:sns:...", Message=json.dumps(alert_payload))
