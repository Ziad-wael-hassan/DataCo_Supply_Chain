from datetime import datetime
from sqlalchemy import create_engine, text
from .config import POSTGRES_URI
from .logger import log

def log_notification(
    dag_id: str,
    run_id: str,
    status: str,
    notification_type: str,
    telegram_status: str,
    message_length: int,
    error_message: str = None
) -> None:
    """
    Persists notification history to warehouse.notifications_log.
    If the table doesn't exist, logs the error and gracefully continues.
    """
    if not POSTGRES_URI:
        log.warning("POSTGRES_URI not set. Skipping notification logging.")
        return

    try:
        engine = create_engine(POSTGRES_URI)
        with engine.begin() as conn:
            query = text("""
                INSERT INTO warehouse.notifications_log (
                    dag_id, run_id, status, notification_type,
                    telegram_status, sent_at, message_length, error_message
                )
                VALUES (
                    :dag_id, :run_id, :status, :notification_type,
                    :telegram_status, :sent_at, :message_length, :error_message
                )
            """)
            conn.execute(query, {
                "dag_id": dag_id or "unknown",
                "run_id": run_id or "unknown",
                "status": status,
                "notification_type": notification_type,
                "telegram_status": telegram_status,
                "sent_at": datetime.utcnow(),
                "message_length": message_length,
                "error_message": error_message,
            })
    except Exception as e:
        log.warning(f"Failed to log notification to database: {e}")
