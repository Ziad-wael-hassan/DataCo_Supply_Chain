from datetime import datetime, timezone
from typing import Any, Dict, Optional
from .logger import log
from .notifier import (
    send_pipeline_started,
    send_pipeline_success,
    send_pipeline_failure
)


def _extract_run_type(dag_run) -> Optional[str]:
    run_type = getattr(dag_run, "run_type", None)
    if hasattr(run_type, "value"):
        return str(run_type.value)
    if run_type is None:
        return None
    return str(run_type)


def _format_trigger_time(dt) -> str:
    if dt is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def on_start_callback(context: Dict[str, Any]):
    dag_run = context.get('dag_run')
    if not dag_run:
        return

    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    log.info("Telegram start callback: dag_id=%s run_id=%s", dag_id, run_id)

    run_type = _extract_run_type(dag_run)
    trigger_type = run_type or ("scheduled" if "scheduled" in run_id else "manual")
    trigger_time = _format_trigger_time(dag_run.start_date)

    send_pipeline_started(
        dag_id=dag_id,
        run_id=run_id,
        trigger_type=trigger_type,
        trigger_time=trigger_time,
    )


def on_success_callback(context: Dict[str, Any]):
    dag_run = context.get('dag_run')
    if not dag_run:
        return

    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    log.info("Telegram success callback: dag_id=%s run_id=%s", dag_id, run_id)
    run_type = _extract_run_type(dag_run)

    start_date = dag_run.start_date
    end_date = dag_run.end_date or datetime.now(timezone.utc)
    duration = (end_date - start_date).total_seconds() if start_date else 0.0

    start_time_str = start_date.strftime("%H:%M UTC") if start_date else "Unknown"
    end_time_str = end_date.strftime("%H:%M UTC") if end_date else "Unknown"

    send_pipeline_success(
        dag_id=dag_id,
        run_id=run_id,
        duration=duration,
        start_time=start_time_str,
        end_time=end_time_str,
        run_type=run_type,
    )


def on_failure_callback(context: Dict[str, Any]):
    dag_run = context.get('dag_run')
    if not dag_run:
        return

    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    log.info("Telegram failure callback: dag_id=%s run_id=%s", dag_id, run_id)
    run_type = _extract_run_type(dag_run)

    start_date = dag_run.start_date
    end_date = dag_run.end_date or datetime.now(timezone.utc)
    duration = (end_date - start_date).total_seconds() if start_date else 0.0

    task_instance = context.get('task_instance')
    task_id = task_instance.task_id if task_instance else "Unknown Task"

    exception = context.get('exception', "Unknown Exception")
    try_number = getattr(task_instance, 'try_number', 1) if task_instance else 1
    max_tries = getattr(task_instance, 'max_tries', 0) if task_instance else 0

    logs_url = ""
    if task_instance and hasattr(task_instance, 'log_url'):
        logs_url = task_instance.log_url

    start_time_str = start_date.strftime("%H:%M UTC") if start_date else "Unknown"
    end_time_str = end_date.strftime("%H:%M UTC") if end_date else "Unknown"

    send_pipeline_failure(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        error=str(exception),
        duration=duration,
        start_time=start_time_str,
        end_time=end_time_str,
        logs_url=logs_url,
        run_type=run_type,
        try_number=try_number,
        max_tries=max_tries,
    )
