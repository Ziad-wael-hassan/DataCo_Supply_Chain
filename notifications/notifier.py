from typing import Dict, Any, List, Optional

from .config import PROJECT_NAME, PROJECT_VERSION, ENVIRONMENT, GIT_COMMIT
from .providers import TelegramProvider
from .formatter import build_telegram_message, format_duration
from .history import log_notification
from .metrics import MetricsProvider
from .logger import log

PIPELINE_TITLE = f"{PROJECT_NAME} Pipeline"


def _notification_status(notification_type: str) -> str:
    if notification_type == "started":
        return "STARTED"
    if notification_type == "failure":
        return "FAILED"
    return "SUCCESS"


def _normalize_run_type(run_id: Optional[str], run_type: Optional[str] = None) -> str:
    normalized = (run_type or "").strip().lower()
    if not normalized and run_id:
        normalized = str(run_id).split("__", 1)[0].strip().lower()

    mapping = {
        "scheduled": "Scheduled",
        "manual": "Manual",
        "backfill": "Backfill",
        "dataset_triggered": "Dataset Triggered",
    }
    return mapping.get(normalized, normalized.replace("_", " ").title() if normalized else "Unknown")


def _send(
    title: str,
    icon: str,
    sections: List[Dict[str, Any]],
    notification_type: str,
    dag_id: str = None,
    run_id: str = None
):
    log.info(
        "Sending Telegram notification: type=%s title=%s dag_id=%s run_id=%s",
        notification_type,
        title,
        dag_id or "n/a",
        run_id or "n/a",
    )
    message = build_telegram_message(title, icon, sections)
    provider = TelegramProvider()
    success, error_msg = provider.send(message)

    status_str = _notification_status(notification_type)
    telegram_status = "sent" if success else "failed"

    log_notification(
        dag_id=dag_id,
        run_id=run_id,
        status=status_str,
        notification_type=notification_type,
        telegram_status=telegram_status,
        message_length=len(message),
        error_message=error_msg if not success else None
    )
    log.info(
        "Telegram notification result: type=%s status=%s provider_status=%s",
        notification_type,
        status_str,
        telegram_status,
    )


def send_pipeline_started(
    dag_id: str,
    run_id: str,
    trigger_type: str,
    trigger_time: str,
):
    pretty_run = _normalize_run_type(run_id, trigger_type)
    sections = [
        {
            "items": {
                "Status": "STARTED",
                "Environment": ENVIRONMENT,
                "Started": trigger_time,
                "Run": pretty_run,
            }
        },
        {
            "title": "DAG",
            "items": {
                "DAG": dag_id,
            },
            "style": "stacked",
        },
    ]
    _send(PIPELINE_TITLE, "🚀", sections, notification_type="started", dag_id=dag_id, run_id=run_id)


def send_pipeline_success(
    dag_id: str,
    run_id: str,
    duration: float,
    start_time: str = None,
    end_time: str = None,
    run_type: str = None,
):
    metrics_provider = MetricsProvider()
    pipeline_metrics = metrics_provider.get_pipeline_metrics()

    duration_str = format_duration(duration)
    sections = [
        {
            "items": {
                "Status": "SUCCESS",
                "Duration": duration_str,
                "Started": start_time,
                "Finished": end_time,
                "Environment": ENVIRONMENT,
            }
        },
    ]

    if pipeline_metrics:
        sections.append({
            "title": "Pipeline Summary",
            "icon": "📊",
            "items": pipeline_metrics,
            "style": "compact_table",
        })

    sections.append({
        "items": {
            "Model Version": PROJECT_VERSION,
        }
    })

    _send(PIPELINE_TITLE, "✅", sections, notification_type="success", dag_id=dag_id, run_id=run_id)


def send_pipeline_failure(
    dag_id: str,
    run_id: str,
    task_id: str,
    error: str,
    duration: float,
    start_time: str = None,
    end_time: str = None,
    logs_url: str = None,
    run_type: str = None,
    try_number: int = 1,
    max_tries: int = 0,
):
    retry_str = f"{try_number} / {max_tries + 1}" if max_tries else str(try_number)

    sections = [
        {
            "items": {
                "Status": "FAILED",
                "Failed Task": task_id,
                "Exception": str(error)[:200] + ("..." if len(str(error)) > 200 else ""),
                "Retry": retry_str,
            },
            "style": "stacked",
        },
    ]

    if logs_url:
        sections.append({
            "title": "Logs",
            "icon": "🔗",
            "items": {
                "Logs": logs_url,
            },
        })

    _send(PIPELINE_TITLE, "❌", sections, notification_type="failure", dag_id=dag_id, run_id=run_id)


def send_model_training(model_name: str, model_version: str, metrics: Dict[str, Any]):
    ordered_metrics = {
        "Model": model_name,
        "Version": model_version,
    }

    metric_priority = [
        "Threshold",
        "ROC-AUC",
        "Precision",
        "Recall",
        "F1",
        "Features",
    ]
    for key in metric_priority:
        if key in metrics:
            ordered_metrics[key] = metrics[key]

    for key, value in metrics.items():
        if key not in metric_priority:
            ordered_metrics[key] = value

    sections = [
        {
            "items": ordered_metrics,
            "style": "stacked",
        }
    ]
    _send(f"Model Retrained", "🤖", sections, notification_type="model_training")


def send_custom_message(title: str, message_dict: Dict[str, Any], icon: str = "ℹ️"):
    sections = [
        {
            "title": title,
            "icon": icon,
            "items": message_dict
        }
    ]
    _send(f"{PROJECT_NAME} - Message", icon, sections, notification_type="custom")
