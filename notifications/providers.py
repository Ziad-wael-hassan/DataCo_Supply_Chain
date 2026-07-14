import time
import requests
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple

from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from .logger import log

class NotificationProvider(ABC):
    @abstractmethod
    def send(self, message: str) -> Tuple[bool, str]:
        """
        Sends the notification. 
        Returns (success_boolean, error_message_or_empty)
        """
        pass

class TelegramProvider(NotificationProvider):
    def __init__(self, bot_token: str = None, chat_id: str = None):
        self.bot_token = bot_token or TELEGRAM_BOT_TOKEN
        self.chat_id = chat_id or TELEGRAM_CHAT_ID
        
    def send(self, message: str) -> Tuple[bool, str]:
        if not self.bot_token or not self.chat_id:
            msg = "Telegram credentials not set. Skipping notification."
            log.warning(msg)
            return False, msg
            
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        
        retries = [1, 2, 5]
        for attempt, backoff in enumerate(retries, start=1):
            try:
                response = requests.post(url, json=payload, timeout=10)
                response.raise_for_status()
                log.info("Successfully sent Telegram notification.")
                return True, ""
            except requests.exceptions.RequestException as e:
                log.warning(f"Telegram API request failed (attempt {attempt}/{len(retries)}): {e}")
                if attempt < len(retries):
                    time.sleep(backoff)
                else:
                    msg = f"Failed to send Telegram message after {len(retries)} attempts: {e}"
                    log.error(msg)
                    return False, msg
        
        return False, "Unknown error during Telegram send"
