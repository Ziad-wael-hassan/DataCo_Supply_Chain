from html import escape
from typing import Any, List, Mapping
from datetime import timedelta

def format_duration(seconds: float) -> str:
    """Returns human readable duration like '2m 17s'"""
    if seconds is None:
        return "Unknown"
    td = timedelta(seconds=int(seconds))
    mins, secs = divmod(td.seconds, 60)
    hours, mins = divmod(mins, 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if mins > 0:
        parts.append(f"{mins}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return " ".join(parts)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in {"", "unknown"}:
        return True
    return False


def _format_compact_table(items: Mapping[str, Any]) -> str:
    visible_items = [(str(k), str(v)) for k, v in items.items() if not _is_missing(v)]
    if not visible_items:
        return ""

    label_width = max(len(label) for label, _ in visible_items)
    table_width = max(24, label_width + 14)
    lines = []

    for label, value in visible_items:
        dot_count = max(2, table_width - len(label) - 1)
        lines.append(f"{label} {'.' * dot_count} {value}")

    return f"<pre>{escape(chr(10).join(lines))}</pre>"

def build_telegram_message(
    title: str, 
    icon: str, 
    sections: List[Mapping[str, Any]]
) -> str:
    """
    Builds a clean Telegram HTML message.
    sections is a list of dicts: {"title": "Status", "icon": "✅", "items": {"Rows": "100"}}
    """
    lines = []
    lines.append(f"{icon} <b>{escape(title)}</b>")
    
    for i, sec in enumerate(sections):
        items = sec.get("items", {})
        visible_items = {k: v for k, v in items.items() if not _is_missing(v)}
        if not visible_items and not sec.get("title"):
            continue

        lines.append("")
        if i > 0 and sec.get("title"):
            lines.append("━━━━━━━━━━━━━━━━━━━━━━")
            lines.append("")
            
        sec_title = sec.get("title")
        sec_icon = sec.get("icon", "")
        style = sec.get("style", "default")
        
        if sec_title:
            title_text = escape(str(sec_title))
            if sec_icon:
                lines.append(f"{sec_icon} <b>{title_text}</b>")
            else:
                lines.append(f"<b>{title_text}</b>")

        if not visible_items:
            continue

        if style == "compact_table":
            compact_table = _format_compact_table(visible_items)
            if compact_table:
                lines.append(compact_table)
            continue

        for index, (k, v) in enumerate(visible_items.items()):
            key = escape(str(k))

            if style == "stacked":
                if index > 0:
                    lines.append("")
                if str(v).startswith("http"):
                    value = escape(str(v), quote=True)
                    lines.append(f"<b>{key}:</b>")
                    lines.append(f"<a href='{value}'>Link</a>")
                else:
                    lines.append(f"<b>{key}:</b>")
                    lines.append(escape(str(v)))
                continue

            if str(v).startswith("http"):
                value = escape(str(v), quote=True)
                lines.append(f"<b>{key}</b>: <a href='{value}'>Link</a>")
            else:
                lines.append(f"<b>{key}</b>: {escape(str(v))}")
        
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines).strip()
