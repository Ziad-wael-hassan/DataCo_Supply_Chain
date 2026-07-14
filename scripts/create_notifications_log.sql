CREATE TABLE IF NOT EXISTS warehouse.notifications_log (
    notification_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255),
    run_id VARCHAR(255),
    status VARCHAR(50),
    notification_type VARCHAR(50),
    telegram_status VARCHAR(50),
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    message_length INT,
    error_message TEXT
);
